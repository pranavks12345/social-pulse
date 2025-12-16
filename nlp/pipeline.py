"""
NLP Pipeline
============
Sentiment analysis, topic extraction, entity recognition, viral prediction.
"""

import re
from typing import List, Dict, Tuple, Optional
from collections import Counter
from dataclasses import dataclass
import math

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False

try:
    import spacy
    nlp = spacy.load("en_core_web_sm")
    SPACY_AVAILABLE = True
except:
    SPACY_AVAILABLE = False
    nlp = None


@dataclass
class NLPResult:
    sentiment_score: float      # -1 to 1
    sentiment_label: str        # negative, neutral, positive
    topics: List[str]           # extracted topics
    entities: List[Dict]        # named entities
    keywords: List[str]         # key terms
    viral_score: float          # 0 to 1 prediction
    engagement_prediction: str  # low, medium, high


class SentimentAnalyzer:
    """VADER-based sentiment analysis with fallback."""
    
    # Fallback word lists if VADER unavailable
    POSITIVE_WORDS = {
        "good", "great", "awesome", "amazing", "excellent", "love", "best",
        "happy", "wonderful", "fantastic", "beautiful", "perfect", "nice",
        "brilliant", "outstanding", "superb", "incredible", "exciting",
        "breakthrough", "innovative", "revolutionary", "success", "win"
    }
    
    NEGATIVE_WORDS = {
        "bad", "terrible", "awful", "horrible", "hate", "worst", "poor",
        "sad", "disappointing", "failure", "crash", "crisis", "disaster",
        "scam", "fraud", "broke", "dead", "dying", "killed", "threat",
        "dangerous", "toxic", "evil", "corrupt", "lawsuit", "fired"
    }
    
    INTENSIFIERS = {"very", "really", "extremely", "absolutely", "totally"}
    NEGATORS = {"not", "no", "never", "neither", "nobody", "nothing"}
    
    def __init__(self):
        self.vader = SentimentIntensityAnalyzer() if VADER_AVAILABLE else None
    
    def analyze(self, text: str) -> Tuple[float, str]:
        """Analyze sentiment of text. Returns (score, label)."""
        if not text:
            return 0.0, "neutral"
        
        if self.vader:
            scores = self.vader.polarity_scores(text)
            compound = scores['compound']
        else:
            compound = self._fallback_sentiment(text)
        
        if compound >= 0.05:
            label = "positive"
        elif compound <= -0.05:
            label = "negative"
        else:
            label = "neutral"
        
        return compound, label
    
    def _fallback_sentiment(self, text: str) -> float:
        """Simple word-based sentiment when VADER unavailable."""
        words = text.lower().split()
        
        pos_count = sum(1 for w in words if w in self.POSITIVE_WORDS)
        neg_count = sum(1 for w in words if w in self.NEGATIVE_WORDS)
        
        total = pos_count + neg_count
        if total == 0:
            return 0.0
        
        return (pos_count - neg_count) / total


class TopicExtractor:
    """Extract topics and keywords from text."""
    
    # Topic categories and their keywords
    TOPIC_PATTERNS = {
        "AI/ML": ["ai", "artificial intelligence", "machine learning", "ml", "gpt", 
                  "chatgpt", "llm", "neural", "deep learning", "openai", "anthropic"],
        "Crypto": ["bitcoin", "btc", "ethereum", "eth", "crypto", "blockchain",
                   "nft", "defi", "web3", "token", "coin"],
        "Finance": ["stock", "market", "invest", "trading", "fed", "inflation",
                    "recession", "economy", "bank", "interest rate"],
        "Tech Industry": ["google", "apple", "microsoft", "amazon", "meta", "facebook",
                         "twitter", "startup", "layoff", "hiring", "ipo"],
        "Programming": ["python", "javascript", "rust", "golang", "react", "api",
                       "framework", "database", "cloud", "aws", "developer"],
        "Politics": ["election", "congress", "senate", "president", "democrat",
                    "republican", "vote", "policy", "government", "law"],
        "Science": ["research", "study", "scientist", "discovery", "nasa", "space",
                   "climate", "physics", "biology", "medical"],
        "Gaming": ["game", "gaming", "playstation", "xbox", "nintendo", "steam",
                  "esports", "gamer", "console", "pc gaming"],
    }
    
    STOPWORDS = {
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "do", "does", "did", "will", "would", "could",
        "should", "may", "might", "must", "shall", "can", "to", "of", "in",
        "for", "on", "with", "at", "by", "from", "as", "into", "through",
        "during", "before", "after", "above", "below", "between", "under",
        "again", "further", "then", "once", "here", "there", "when", "where",
        "why", "how", "all", "each", "few", "more", "most", "other", "some",
        "such", "no", "nor", "not", "only", "own", "same", "so", "than",
        "too", "very", "just", "and", "but", "if", "or", "because", "until",
        "while", "this", "that", "these", "those", "i", "you", "he", "she",
        "it", "we", "they", "what", "which", "who", "whom", "its", "his",
        "her", "their", "my", "your", "our", "about", "get", "got", "like",
        "know", "think", "make", "see", "look", "want", "give", "use", "find",
        "tell", "ask", "work", "seem", "feel", "try", "leave", "call", "http",
        "https", "www", "com", "org", "reddit", "deleted", "removed"
    }
    
    def extract_topics(self, text: str) -> List[str]:
        """Extract topic categories from text."""
        text_lower = text.lower()
        topics = []
        
        for topic, keywords in self.TOPIC_PATTERNS.items():
            if any(kw in text_lower for kw in keywords):
                topics.append(topic)
        
        return topics if topics else ["General"]
    
    def extract_keywords(self, text: str, top_n: int = 10) -> List[str]:
        """Extract top keywords from text."""
        # Clean text
        text = re.sub(r'http\S+', '', text)
        text = re.sub(r'[^\w\s]', ' ', text.lower())
        
        words = text.split()
        words = [w for w in words if w not in self.STOPWORDS and len(w) > 2]
        
        # Count and return top
        counts = Counter(words)
        return [word for word, _ in counts.most_common(top_n)]
    
    def extract_entities(self, text: str) -> List[Dict]:
        """Extract named entities using spaCy."""
        if not SPACY_AVAILABLE or not nlp:
            return []
        
        doc = nlp(text[:10000])  # Limit length
        
        entities = []
        seen = set()
        for ent in doc.ents:
            if ent.text.lower() not in seen and ent.label_ in ["ORG", "PERSON", "GPE", "PRODUCT"]:
                seen.add(ent.text.lower())
                entities.append({
                    "text": ent.text,
                    "type": ent.label_
                })
        
        return entities[:20]


class ViralPredictor:
    """Predict viral potential of content."""
    
    # Features that correlate with virality
    VIRAL_PATTERNS = [
        r"\?$",                          # Questions
        r"^(how|why|what|when|where)",   # How-to, explainers
        r"(breaking|just in|update)",    # News/urgency
        r"(first|new|latest|launch)",    # Novelty
        r"(secret|hidden|revealed)",     # Mystery/curiosity
        r"(you won't believe|amazing)",  # Clickbait (works)
        r"\d+\s*(tips|ways|reasons)",    # Listicles
        r"(ask me anything|ama)",        # Interactive
    ]
    
    ENGAGEMENT_WORDS = {
        "you", "your", "free", "new", "now", "how", "why", "best",
        "top", "first", "exclusive", "limited", "easy", "simple"
    }
    
    def predict(self, title: str, body: str, score: int, num_comments: int, 
                age_hours: float) -> Tuple[float, str]:
        """Predict viral potential. Returns (score 0-1, engagement level)."""
        
        text = f"{title} {body}".lower()
        features = []
        
        # Pattern matches
        pattern_score = sum(1 for p in self.VIRAL_PATTERNS if re.search(p, title.lower()))
        features.append(min(pattern_score / 3, 1.0))
        
        # Engagement words
        words = text.split()
        eng_count = sum(1 for w in words if w in self.ENGAGEMENT_WORDS)
        features.append(min(eng_count / 5, 1.0))
        
        # Title length (sweet spot: 60-100 chars)
        title_len = len(title)
        if 60 <= title_len <= 100:
            features.append(1.0)
        elif 40 <= title_len <= 120:
            features.append(0.7)
        else:
            features.append(0.3)
        
        # Early velocity (score / age)
        if age_hours > 0:
            velocity = score / age_hours
            velocity_score = min(velocity / 50, 1.0)
            features.append(velocity_score)
        else:
            features.append(0.5)
        
        # Comment ratio
        if score > 0:
            comment_ratio = num_comments / score
            # High comment ratio = controversial/engaging
            features.append(min(comment_ratio * 2, 1.0))
        else:
            features.append(0.5)
        
        # Current traction
        if score > 1000:
            features.append(1.0)
        elif score > 100:
            features.append(0.7)
        elif score > 10:
            features.append(0.4)
        else:
            features.append(0.2)
        
        # Calculate final score
        viral_score = sum(features) / len(features)
        
        # Engagement prediction
        if viral_score >= 0.7:
            engagement = "high"
        elif viral_score >= 0.4:
            engagement = "medium"
        else:
            engagement = "low"
        
        return viral_score, engagement


class NLPPipeline:
    """Complete NLP pipeline."""
    
    def __init__(self):
        self.sentiment = SentimentAnalyzer()
        self.topics = TopicExtractor()
        self.viral = ViralPredictor()
    
    def analyze(self, title: str, body: str = "", score: int = 0, 
                num_comments: int = 0, age_hours: float = 1.0) -> NLPResult:
        """Run full NLP analysis on content."""
        
        text = f"{title} {body}"
        
        # Sentiment
        sent_score, sent_label = self.sentiment.analyze(text)
        
        # Topics and entities
        topics = self.topics.extract_topics(text)
        keywords = self.topics.extract_keywords(text)
        entities = self.topics.extract_entities(text)
        
        # Viral prediction
        viral_score, engagement = self.viral.predict(
            title, body, score, num_comments, age_hours
        )
        
        return NLPResult(
            sentiment_score=round(sent_score, 3),
            sentiment_label=sent_label,
            topics=topics,
            entities=entities,
            keywords=keywords,
            viral_score=round(viral_score, 3),
            engagement_prediction=engagement
        )
    
    def analyze_batch(self, items: List[Dict]) -> List[NLPResult]:
        """Analyze multiple items."""
        results = []
        for item in items:
            result = self.analyze(
                title=item.get("title", ""),
                body=item.get("body", item.get("text", "")),
                score=item.get("score", 0),
                num_comments=item.get("num_comments", 0),
                age_hours=item.get("age_hours", 1.0)
            )
            results.append(result)
        return results


def main():
    """Test NLP pipeline."""
    pipeline = NLPPipeline()
    
    test_posts = [
        {
            "title": "OpenAI announces GPT-5 with breakthrough reasoning capabilities",
            "body": "The new model shows amazing performance on complex tasks.",
            "score": 5000,
            "num_comments": 1200,
            "age_hours": 3
        },
        {
            "title": "Bitcoin crashes 20% amid regulatory concerns",
            "body": "Investors are worried about new cryptocurrency regulations.",
            "score": 2000,
            "num_comments": 800,
            "age_hours": 2
        },
        {
            "title": "How I learned Python in 30 days",
            "body": "Here are my tips for learning programming quickly.",
            "score": 150,
            "num_comments": 45,
            "age_hours": 12
        }
    ]
    
    print("Testing NLP Pipeline...")
    print("=" * 60)
    
    for post in test_posts:
        result = pipeline.analyze(**post)
        print(f"\nTitle: {post['title'][:50]}...")
        print(f"  Sentiment: {result.sentiment_label} ({result.sentiment_score})")
        print(f"  Topics: {result.topics}")
        print(f"  Keywords: {result.keywords[:5]}")
        print(f"  Viral Score: {result.viral_score} ({result.engagement_prediction})")


if __name__ == "__main__":
    main()
