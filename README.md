# GDELT Iceberg Project

This project leverages the [GDELT](https://www.gdeltproject.org/) news database to demonstrate the use of [Apache Iceberg](https://iceberg.apache.org/) with a PostgreSQL catalog and Kafka for data streaming.

## Features

- Integrates GDELT data with Apache Iceberg tables
- Uses PostgreSQL as the Iceberg catalog
- Streams data via Kafka (producer and consumer)
- Containerized setup using Docker

## Requirements

- [UV](https://github.com/astral-sh/uv)
- [Docker](https://www.docker.com/)
- Docker Compose

## Getting Started

### 1. Start Kafka and PostgreSQL

```bash
docker compose up
```

### 2. Run Producer and Consumer

Open two terminals and execute:

**Producer:**
```bash
uv run ./iceberg_services/kafka_producer.py
```

**Consumer:**
```bash
uv run ./iceberg_services/kafka_consumer.py
```

## Project Structure

```
.
├── iceberg_services/
│   ├── kafka_producer.py
│   └── kafka_consumer.py
├── docker-compose.yml
└── README.md
```
