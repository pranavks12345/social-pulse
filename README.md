# Social Pulse

Production-grade real-time social media analytics pipeline.



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

### Option 1: Docker 

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
