"""
Social Pulse Dashboard
======================
Real-time social media analytics dashboard.
Run: streamlit run dashboard/app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from database.models import db, Post

# Page config
st.set_page_config(
    page_title="Social Pulse",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%);
        border-radius: 10px;
        padding: 1rem;
        border: 1px solid #3d3d5c;
    }
    .stMetric {
        background-color: #1e1e2e;
        padding: 1rem;
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=300)
def load_data(hours: int = 24):
    """Load recent posts from database."""
    with db.session() as session:
        posts = db.get_recent_posts(session, hours=hours, limit=5000)
        
        if not posts:
            return pd.DataFrame()
        
        data = []
        for p in posts:
            data.append({
                "id": p.id,
                "source": p.source,
                "title": p.title,
                "score": p.score or 0,
                "num_comments": p.num_comments or 0,
                "sentiment_score": p.sentiment_score or 0,
                "sentiment_label": p.sentiment_label or "neutral",
                "viral_score": p.viral_score or 0,
                "engagement_prediction": p.engagement_prediction or "low",
                "topics": p.topics or [],
                "keywords": p.keywords or [],
                "subreddit": p.subreddit,
                "created_at": p.created_at,
                "scraped_at": p.scraped_at,
                "url": p.url
            })
        
        return pd.DataFrame(data)


def render_sidebar():
    """Render sidebar filters."""
    st.sidebar.markdown("## 🎛️ Filters")
    
    hours = st.sidebar.slider("Time Range (hours)", 1, 72, 24)
    
    sources = st.sidebar.multiselect(
        "Sources",
        ["reddit", "hackernews"],
        default=["reddit", "hackernews"]
    )
    
    sentiment = st.sidebar.multiselect(
        "Sentiment",
        ["positive", "neutral", "negative"],
        default=["positive", "neutral", "negative"]
    )
    
    min_score = st.sidebar.number_input("Minimum Score", 0, 10000, 0)
    
    return hours, sources, sentiment, min_score


def render_metrics(df: pd.DataFrame):
    """Render top metrics."""
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("📝 Total Posts", f"{len(df):,}")
    
    with col2:
        avg_sentiment = df['sentiment_score'].mean()
        sentiment_emoji = "😊" if avg_sentiment > 0.1 else "😐" if avg_sentiment > -0.1 else "😔"
        st.metric(f"{sentiment_emoji} Avg Sentiment", f"{avg_sentiment:.2f}")
    
    with col3:
        total_engagement = df['score'].sum() + df['num_comments'].sum()
        st.metric("🔥 Total Engagement", f"{total_engagement:,}")
    
    with col4:
        avg_viral = df['viral_score'].mean()
        st.metric("📈 Avg Viral Score", f"{avg_viral:.2f}")
    
    with col5:
        high_viral = len(df[df['viral_score'] >= 0.6])
        st.metric("🚀 Trending Posts", f"{high_viral:,}")


def render_sentiment_chart(df: pd.DataFrame):
    """Render sentiment over time chart."""
    st.subheader("📊 Sentiment Over Time")
    
    if df.empty or 'scraped_at' not in df.columns:
        st.info("No data available")
        return
    
    df['hour'] = pd.to_datetime(df['scraped_at']).dt.floor('H')
    
    hourly = df.groupby(['hour', 'source']).agg({
        'sentiment_score': 'mean',
        'id': 'count'
    }).reset_index()
    hourly.columns = ['hour', 'source', 'avg_sentiment', 'count']
    
    fig = px.line(
        hourly,
        x='hour',
        y='avg_sentiment',
        color='source',
        markers=True,
        title='Sentiment Trend'
    )
    
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    fig.update_layout(
        template="plotly_dark",
        xaxis_title="Time",
        yaxis_title="Average Sentiment",
        legend_title="Source"
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_topic_chart(df: pd.DataFrame):
    """Render topic distribution chart."""
    st.subheader("🏷️ Trending Topics")
    
    if df.empty:
        st.info("No data available")
        return
    
    # Flatten topics
    all_topics = []
    for topics in df['topics']:
        if topics:
            all_topics.extend(topics)
    
    if not all_topics:
        st.info("No topics detected")
        return
    
    topic_counts = pd.Series(all_topics).value_counts().head(10)
    
    fig = px.bar(
        x=topic_counts.values,
        y=topic_counts.index,
        orientation='h',
        title='Top Topics',
        labels={'x': 'Post Count', 'y': 'Topic'}
    )
    
    fig.update_layout(
        template="plotly_dark",
        yaxis={'categoryorder': 'total ascending'}
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_viral_chart(df: pd.DataFrame):
    """Render viral score distribution."""
    st.subheader("🚀 Viral Potential Distribution")
    
    if df.empty:
        st.info("No data available")
        return
    
    fig = px.histogram(
        df,
        x='viral_score',
        color='source',
        nbins=20,
        title='Viral Score Distribution',
        barmode='overlay'
    )
    
    fig.update_layout(
        template="plotly_dark",
        xaxis_title="Viral Score",
        yaxis_title="Count"
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_sentiment_breakdown(df: pd.DataFrame):
    """Render sentiment breakdown pie chart."""
    st.subheader("😊😐😔 Sentiment Breakdown")
    
    if df.empty:
        st.info("No data available")
        return
    
    sentiment_counts = df['sentiment_label'].value_counts()
    
    colors = {
        'positive': '#00cc96',
        'neutral': '#636efa',
        'negative': '#ef553b'
    }
    
    fig = px.pie(
        values=sentiment_counts.values,
        names=sentiment_counts.index,
        title='Sentiment Distribution',
        color=sentiment_counts.index,
        color_discrete_map=colors
    )
    
    fig.update_layout(template="plotly_dark")
    
    st.plotly_chart(fig, use_container_width=True)


def render_top_posts(df: pd.DataFrame):
    """Render top posts table."""
    st.subheader("🔝 Top Posts")
    
    if df.empty:
        st.info("No data available")
        return
    
    tab1, tab2, tab3 = st.tabs(["🔥 By Score", "🚀 By Viral", "💬 By Engagement"])
    
    with tab1:
        top_score = df.nlargest(10, 'score')[['source', 'title', 'score', 'sentiment_label', 'viral_score']]
        st.dataframe(top_score, use_container_width=True)
    
    with tab2:
        top_viral = df.nlargest(10, 'viral_score')[['source', 'title', 'score', 'sentiment_label', 'viral_score']]
        st.dataframe(top_viral, use_container_width=True)
    
    with tab3:
        df['engagement'] = df['score'] + df['num_comments'] * 2
        top_engage = df.nlargest(10, 'engagement')[['source', 'title', 'score', 'num_comments', 'engagement']]
        st.dataframe(top_engage, use_container_width=True)


def render_source_comparison(df: pd.DataFrame):
    """Render source comparison."""
    st.subheader("📊 Source Comparison")
    
    if df.empty:
        st.info("No data available")
        return
    
    comparison = df.groupby('source').agg({
        'id': 'count',
        'score': 'mean',
        'sentiment_score': 'mean',
        'viral_score': 'mean',
        'num_comments': 'mean'
    }).round(2)
    
    comparison.columns = ['Posts', 'Avg Score', 'Avg Sentiment', 'Avg Viral', 'Avg Comments']
    
    st.dataframe(comparison, use_container_width=True)


def render_keyword_cloud(df: pd.DataFrame):
    """Render top keywords."""
    st.subheader("🔤 Top Keywords")
    
    if df.empty:
        st.info("No data available")
        return
    
    all_keywords = []
    for kws in df['keywords']:
        if kws:
            all_keywords.extend(kws[:5])
    
    if not all_keywords:
        st.info("No keywords detected")
        return
    
    kw_counts = pd.Series(all_keywords).value_counts().head(20)
    
    fig = px.bar(
        x=kw_counts.index,
        y=kw_counts.values,
        title='Top Keywords',
        labels={'x': 'Keyword', 'y': 'Count'}
    )
    
    fig.update_layout(
        template="plotly_dark",
        xaxis_tickangle=-45
    )
    
    st.plotly_chart(fig, use_container_width=True)


def main():
    """Main dashboard."""
    st.markdown('<h1 class="main-header">📊 Social Pulse</h1>', unsafe_allow_html=True)
    st.markdown("Real-time social media analytics dashboard")
    
    # Sidebar
    hours, sources, sentiment, min_score = render_sidebar()
    
    # Load data
    df = load_data(hours)
    
    if df.empty:
        st.warning("⚠️ No data available. Run the scraper first:")
        st.code("python -c \"from orchestration.flows import run_scrape; run_scrape()\"")
        return
    
    # Apply filters
    if sources:
        df = df[df['source'].isin(sources)]
    if sentiment:
        df = df[df['sentiment_label'].isin(sentiment)]
    df = df[df['score'] >= min_score]
    
    if df.empty:
        st.warning("No posts match your filters")
        return
    
    # Metrics row
    render_metrics(df)
    
    st.divider()
    
    # Charts - Row 1
    col1, col2 = st.columns(2)
    with col1:
        render_sentiment_chart(df)
    with col2:
        render_topic_chart(df)
    
    # Charts - Row 2
    col1, col2 = st.columns(2)
    with col1:
        render_viral_chart(df)
    with col2:
        render_sentiment_breakdown(df)
    
    st.divider()
    
    # Charts - Row 3
    col1, col2 = st.columns(2)
    with col1:
        render_keyword_cloud(df)
    with col2:
        render_source_comparison(df)
    
    st.divider()
    
    # Top posts
    render_top_posts(df)
    
    # Footer
    st.divider()
    st.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
    st.markdown("*Data from Reddit & HackerNews*")


if __name__ == "__main__":
    main()
