# Data Processing API

A backend service for ingesting data from RESTful and GraphQL APIs, transforming and storing it in a database, and exposing the processed data via a RESTful API.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Running the API Server](#running-the-api-server)
  - [Endpoints](#endpoints)
  - [Triggering Data Processing](#triggering-data-processing)
  - [Scheduler](#scheduler)
- [Metrics & Monitoring](#metrics--monitoring)
- [Logging](#logging)

## Features

- **Asynchronous Data Ingestion**: Fetch data concurrently from multiple REST and GraphQL APIs.
- **Data Transformation**: Normalize and flatten JSON payloads into tabular format.
- **Scalable Storage**: Store data in MySQL (or any SQL database) with chunked inserts.
- **RESTful API**: Expose processed data with pagination, sorting, and filtering.
- **On-Demand Processing**: Trigger data ingestion at runtime via HTTP endpoints.
- **Scheduled Jobs**: Periodic data refresh using a built-in scheduler.
- **Metrics & Monitoring**: Prometheus metrics for fetch counts, processing time, and storage operations.
- **Structured Logging**: Rotating file logs with console output.

## Architecture

```plaintext
┌───────────────┐       ┌───────────────┐       ┌───────────────┐
│ External APIs │  →→→  │ Data Processor│  →→→  │   Database    │
│ (REST/GraphQL)│       │ (async + sync)│       │   (MySQL)     │
└───────────────┘       └───────────────┘       └───────────────┘
        ↑                                          ↓
        │                                          │
        │                                          ▼
     Scheduler  ─── triggers ──▶  API Server (FastAPI)
                                         │
                                         ▼
                                  Client Applications
```

- **Scheduler**: `scheduler.py` uses `schedule` to run ingestion jobs at regular intervals.
- **Data Processor**: `data_processor.py` handles API fetching, transformation, and storage.
- **API Server**: `main.py` (FastAPI) serves data and provides endpoints to trigger processing.

## Prerequisites

- Python 3.8+
- MySQL server (or compatible SQL database)
- `pip` for Python package installation

## Installation

1. **Create a virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Create `.env` and set the following variables:

```ini
# Database
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DB=data_processing
MYSQL_USERNAME=your_username
MYSQL_PASSWORD=your_password

# API Server
API_PORT=8080
API_KEY=your_api_key
CORS_ORIGINS=*

# Data Processor
API_RATE_LIMIT=10
WORKER_THREADS=4
METRICS_PORT=8000
```

## Usage

### Running the API Server

```bash
uvicorn main:app --reload --host 0.0.0.0 --port ${API_PORT}
```

### Endpoints

- **GET /**: Basic info
- **GET /health**: Health check
- **GET /tables**: List available tables (requires `X-API-Key`)
- **GET /data/{table_name}**: Query data with pagination and sorting (requires `X-API-Key`)
- **POST /process**: Trigger on-demand data processing (requires `X-API-Key`)

#### Example: List Tables
```bash
curl -H "X-API-Key: ${API_KEY}" http://localhost:8080/tables
```

#### Example: Query Table Data
```bash
curl "http://localhost:8080/data/posts?limit=10&offset=0" \
     -H "X-API-Key: ${API_KEY}"
```

### Triggering Data Processing

```bash
curl -X POST http://localhost:8080/process \
     -H "Content-Type: application/json" \
     -H "X-API-Key: ${API_KEY}" \
     -d '{
           "apis": [
             {"url": "https://jsonplaceholder.typicode.com/posts","api_type": "REST","label": "posts"}
           ]
         }'
```

### Scheduler

To start the periodic ingestion scheduler:

```bash
python scheduler.py
```

## Metrics & Monitoring

Prometheus metrics are available at `http://localhost:${METRICS_PORT}/metrics`, including:

- `api_fetch_total{api,status}`
- `transform_processing_seconds{api}`
- `storage_processing_seconds{api,status}`
- `rows_processed_total{api}`
- `api_requests_total{endpoint,method,status}`
- `api_request_latency_seconds{endpoint}`

## Logging

- Logs are written to `logs/` with rotation (max 5 files, 10 MB each).
- Console logging is also enabled for real-time debugging.

