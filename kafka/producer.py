"""
Kafka Producer
==============
Publishes scraped posts to Kafka topics for real-time processing.
"""

import json
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
import os

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Topics
TOPICS = {
    "raw_posts": "social.raw.posts",
    "processed_posts": "social.processed.posts",
    "trending": "social.trending",
    "alerts": "social.alerts"
}


class KafkaProducerClient:
    """Kafka producer for publishing social media posts."""
    
    def __init__(self):
        self.config = {
            'bootstrap.servers': KAFKA_SERVERS,
            'client.id': 'social-pulse-producer',
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 1000,
            'compression.type': 'gzip',
            'batch.size': 16384,
            'linger.ms': 10
        }
        self.producer = None
        self.delivery_count = 0
        self.error_count = 0
    
    def connect(self):
        """Initialize producer connection."""
        self.producer = Producer(self.config)
        self._ensure_topics()
        print(f"✅ Kafka producer connected to {KAFKA_SERVERS}")
    
    def _ensure_topics(self):
        """Create topics if they don't exist."""
        admin = AdminClient({'bootstrap.servers': KAFKA_SERVERS})
        
        existing = admin.list_topics(timeout=10).topics.keys()
        
        new_topics = [
            NewTopic(topic, num_partitions=3, replication_factor=1)
            for topic in TOPICS.values()
            if topic not in existing
        ]
        
        if new_topics:
            futures = admin.create_topics(new_topics)
            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"  Created topic: {topic}")
                except Exception as e:
                    if "already exists" not in str(e):
                        print(f"  Error creating {topic}: {e}")
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err:
            self.error_count += 1
            print(f"❌ Delivery failed: {err}")
        else:
            self.delivery_count += 1
    
    def publish(self, topic: str, key: str, value: Dict):
        """Publish a single message."""
        if not self.producer:
            self.connect()
        
        # Add metadata
        value['_published_at'] = datetime.now().isoformat()
        value['_topic'] = topic
        
        self.producer.produce(
            topic=topic,
            key=key.encode('utf-8'),
            value=json.dumps(value).encode('utf-8'),
            callback=self._delivery_callback
        )
        
        # Trigger delivery
        self.producer.poll(0)
    
    def publish_batch(self, topic: str, messages: List[Dict]):
        """Publish multiple messages."""
        for msg in messages:
            key = msg.get('id', msg.get('external_id', 'unknown'))
            self.publish(topic, str(key), msg)
        
        # Flush all messages
        self.producer.flush()
        print(f"📤 Published {len(messages)} messages to {topic}")
    
    def publish_post(self, post: Dict, source: str):
        """Publish a raw post to processing queue."""
        post['source'] = source
        key = f"{source}_{post.get('id', 'unknown')}"
        self.publish(TOPICS['raw_posts'], key, post)
    
    def publish_processed(self, post: Dict):
        """Publish a processed post."""
        key = post.get('external_id', 'unknown')
        self.publish(TOPICS['processed_posts'], key, post)
    
    def publish_trending(self, trending_data: Dict):
        """Publish trending snapshot."""
        key = f"trending_{datetime.now().strftime('%Y%m%d_%H%M')}"
        self.publish(TOPICS['trending'], key, trending_data)
    
    def publish_alert(self, alert: Dict):
        """Publish an alert (viral content, sentiment spike, etc)."""
        key = f"alert_{alert.get('type', 'unknown')}_{datetime.now().timestamp()}"
        self.publish(TOPICS['alerts'], key, alert)
    
    def close(self):
        """Close producer connection."""
        if self.producer:
            self.producer.flush()
            print(f"📊 Producer stats: {self.delivery_count} delivered, {self.error_count} errors")


# Singleton instance
producer = KafkaProducerClient()


def get_producer() -> KafkaProducerClient:
    """Get producer instance."""
    if not producer.producer:
        producer.connect()
    return producer
