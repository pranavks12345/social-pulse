"""
Kafka Consumer
==============
Consumes raw posts from Kafka, processes with NLP, stores to database.
Real-time stream processing.
"""

import json
import os
import signal
import sys
from datetime import datetime
from typing import Dict, Optional
from threading import Event

from confluent_kafka import Consumer, KafkaError, KafkaException

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nlp.pipeline import NLPPipeline
from database.models import db, Post
from kafka.producer import get_producer, TOPICS


KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


class StreamProcessor:
    """Real-time stream processor for social media posts."""
    
    def __init__(self, group_id: str = "social-pulse-processors"):
        self.config = {
            'bootstrap.servers': KAFKA_SERVERS,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            'session.timeout.ms': 30000,
            'max.poll.interval.ms': 300000
        }
        self.consumer = None
        self.nlp = NLPPipeline()
        self.producer = get_producer()
        self.running = Event()
        self.processed_count = 0
        self.error_count = 0
        
        # Metrics for monitoring
        self.metrics = {
            'processed': 0,
            'errors': 0,
            'avg_processing_time': 0,
            'last_processed': None
        }
    
    def connect(self):
        """Initialize consumer connection."""
        self.consumer = Consumer(self.config)
        self.consumer.subscribe([TOPICS['raw_posts']])
        self.running.set()
        print(f"✅ Consumer connected, subscribed to {TOPICS['raw_posts']}")
    
    def process_message(self, msg) -> Optional[Dict]:
        """Process a single message."""
        try:
            value = json.loads(msg.value().decode('utf-8'))
            source = value.get('source', 'unknown')
            
            # Extract text for NLP
            title = value.get('title', '')
            body = value.get('body', value.get('text', ''))
            score = value.get('score', 0)
            num_comments = value.get('num_comments', 0)
            
            # Calculate age
            created = value.get('created_utc')
            if isinstance(created, str):
                try:
                    created_dt = datetime.fromisoformat(created)
                except:
                    created_dt = datetime.now()
            else:
                created_dt = datetime.now()
            
            age_hours = max((datetime.now() - created_dt).total_seconds() / 3600, 0.1)
            
            # Run NLP
            nlp_result = self.nlp.analyze(
                title=title,
                body=body,
                score=score,
                num_comments=num_comments,
                age_hours=age_hours
            )
            
            # Build processed post
            processed = {
                'external_id': f"{source}_{value.get('id', '')}",
                'source': source,
                'title': title,
                'body': body,
                'url': value.get('url', ''),
                'author': value.get('author', ''),
                'score': score,
                'num_comments': num_comments,
                'upvote_ratio': value.get('upvote_ratio'),
                'subreddit': value.get('subreddit'),
                'story_type': value.get('story_type'),
                'created_at': created_dt,
                'scraped_at': datetime.now(),
                'sentiment_score': nlp_result.sentiment_score,
                'sentiment_label': nlp_result.sentiment_label,
                'topics': nlp_result.topics,
                'keywords': nlp_result.keywords,
                'entities': nlp_result.entities,
                'viral_score': nlp_result.viral_score,
                'engagement_prediction': nlp_result.engagement_prediction
            }
            
            return processed
            
        except Exception as e:
            print(f"❌ Error processing message: {e}")
            self.error_count += 1
            return None
    
    def store_post(self, processed: Dict):
        """Store processed post to database."""
        try:
            db.insert_posts_batch([processed])
        except Exception as e:
            print(f"❌ Error storing post: {e}")
    
    def check_alerts(self, processed: Dict):
        """Check for alert conditions."""
        # Viral alert
        if processed['viral_score'] >= 0.8:
            self.producer.publish_alert({
                'type': 'viral',
                'title': processed['title'][:100],
                'viral_score': processed['viral_score'],
                'source': processed['source'],
                'url': processed['url']
            })
        
        # Sentiment spike alert
        if abs(processed['sentiment_score']) >= 0.8:
            self.producer.publish_alert({
                'type': 'sentiment_spike',
                'title': processed['title'][:100],
                'sentiment': processed['sentiment_score'],
                'label': processed['sentiment_label'],
                'source': processed['source']
            })
    
    def run(self):
        """Main processing loop."""
        if not self.consumer:
            self.connect()
        
        print("🚀 Starting stream processor...")
        
        try:
            while self.running.is_set():
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                
                # Process message
                start_time = datetime.now()
                processed = self.process_message(msg)
                
                if processed:
                    # Store to database
                    self.store_post(processed)
                    
                    # Publish processed message
                    self.producer.publish_processed(processed)
                    
                    # Check for alerts
                    self.check_alerts(processed)
                    
                    # Update metrics
                    self.processed_count += 1
                    proc_time = (datetime.now() - start_time).total_seconds()
                    self.metrics['processed'] = self.processed_count
                    self.metrics['avg_processing_time'] = (
                        (self.metrics['avg_processing_time'] * (self.processed_count - 1) + proc_time) 
                        / self.processed_count
                    )
                    self.metrics['last_processed'] = datetime.now().isoformat()
                    
                    if self.processed_count % 100 == 0:
                        print(f"📊 Processed {self.processed_count} posts (avg {self.metrics['avg_processing_time']:.3f}s)")
        
        except KeyboardInterrupt:
            print("\n⏹️ Stopping processor...")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the processor."""
        self.running.clear()
        if self.consumer:
            self.consumer.close()
        print(f"📊 Final stats: {self.processed_count} processed, {self.error_count} errors")


def main():
    """Run the stream processor."""
    db.create_tables()
    
    processor = StreamProcessor()
    
    # Handle shutdown signals
    def signal_handler(sig, frame):
        print("\n⏹️ Received shutdown signal...")
        processor.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    processor.run()


if __name__ == "__main__":
    main()
