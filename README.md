# Social Pulse

Production-grade real-time social media analytics pipeline.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                    │
│                      Reddit │ HackerNews │ (expandable)                     │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STREAMING SCRAPERS                                   │
│                    Async Python │ Rate Limiting │ Continuous                │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              KAFKA                                           │
│              Topics: raw.posts │ processed.posts │ trending │ alerts        │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STREAM PROCESSOR                                     │
│                   NLP Pipeline │ Sentiment │ Topics │ Viral Prediction      │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            POSTGRESQL                                        │
│                     Posts │ Trends │ Entities │ Snapshots                   │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              dbt                                             │
│              staging → intermediate → marts (daily, hourly, top)            │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
┌─────────────────────────────┐   ┌─────────────────────────────┐
│      STREAMLIT DASHBOARD    │   │       FASTAPI + WEBSOCKET   │
│   Charts │ Trends │ Viral   │   │   REST API │ Real-time WS   │
└─────────────────────────────┘   └─────────────────────────────┘
                                              │
                                              ▼
                              ┌─────────────────────────────┐
                              │    PROMETHEUS + GRAFANA     │
                              │        Monitoring           │
                              └─────────────────────────────┘
```

## Features

- **Real-time Streaming**: Kafka-based pipeline, not batch
- **Continuous Scraping**: Always-on scrapers publishing to Kafka
- **NLP Pipeline**: Sentiment, topics, entities, viral prediction
- **dbt Transforms**: Proper data modeling (staging → marts)
- **REST API**: FastAPI with WebSocket for real-time updates
- **Monitoring**: Prometheus + Grafana dashboards
- **Docker**: Fully containerized with Docker Compose
- **Cloud Ready**: Terraform for AWS (ECS, RDS, ElastiCache, MSK)

## Quick Start

### Option 1: Docker (Recommended)

```bash
# Start everything
docker-compose up -d

# View logs
docker-compose logs -f

# Open:
#   Dashboard: http://localhost:8501
#   API: http://localhost:8000
#   Kafka UI: http://localhost:8080
#   Grafana: http://localhost:3000 (admin/admin)
```

### Option 2: Local Development

```bash
# Install
pip install -r requirements.txt
python -m spacy download en_core_web_sm

# Setup + first scrape
python scripts/setup.py

# Run dashboard
python scripts/run.py
```

## Project Structure

```
social-pulse/
├── scrapers/
│   ├── reddit.py           # Async Reddit scraper
│   ├── hackernews.py       # HackerNews API client
│   └── streaming.py        # Continuous scraper → Kafka
├── kafka/
│   ├── producer.py         # Kafka producer client
│   └── consumer.py         # Stream processor
├── nlp/
│   └── pipeline.py         # Sentiment, topics, viral prediction
├── database/
│   ├── models.py           # SQLAlchemy ORM
│   └── init.sql            # PostgreSQL init script
├── dbt/
│   └── models/             # dbt transformations
│       ├── staging/
│       ├── intermediate/
│       └── marts/
├── api/
│   └── server.py           # FastAPI + WebSocket
├── dashboard/
│   └── app.py              # Streamlit dashboard
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/
├── docker/
│   ├── Dockerfile.scraper
│   ├── Dockerfile.processor
│   ├── Dockerfile.api
│   └── Dockerfile.dashboard
├── infra/
│   └── main.tf             # Terraform for AWS
├── docker-compose.yml
└── requirements.txt
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/posts` | GET | Get posts with filters |
| `/trending` | GET | Trending topics |
| `/stats` | GET | Overall statistics |
| `/search` | GET | Search posts by keyword |
| `/viral` | GET | High viral score posts |
| `/sentiment/timeline` | GET | Sentiment over time |
| `/ws` | WebSocket | Real-time updates |

## Tech Stack

| Layer | Technology |
|-------|------------|
| Scraping | Python, aiohttp (async) |
| Message Queue | Kafka |
| Processing | Python, spaCy, VADER |
| Database | PostgreSQL |
| Cache | Redis |
| Transforms | dbt |
| API | FastAPI, WebSocket |
| Dashboard | Streamlit, Plotly |
| Monitoring | Prometheus, Grafana |
| Containers | Docker, Docker Compose |
| Cloud | Terraform, AWS (ECS, RDS, MSK) |

## Deploy to AWS

```bash
cd infra
terraform init
terraform plan
terraform apply
```

Creates:
- VPC with public/private subnets
- ECS Fargate cluster
- RDS PostgreSQL
- ElastiCache Redis
- ECR repositories
- CloudWatch logging
