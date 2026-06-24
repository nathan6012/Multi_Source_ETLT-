# ETL/ELT Data Engineering Pipeline System

![Prefect](https://img.shields.io/badge/Orchestration-Prefect-06b6d4)
![BigQuery](https://img.shields.io/badge/Data%20Warehouse-BigQuery-4285F4?logo=googlecloud)
![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-336791?logo=postgresql)
![Pandas](https://img.shields.io/badge/Data-Pandas-black)
![SQLAlchemy](https://img.shields.io/badge/Database-SQLAlchemy-red)
![Pydantic](https://img.shields.io/badge/Validation-Pydantic-009688?logo=pydantic)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED?logo=docker)
![GitHub Actions](https://img.shields.io/badge/CI/CD-GitHub%20Actions-2088FF?logo=githubactions)

---

# 📌 Overview

This project is a production-style **multi-source ETL/ELT Data Engineering Pipeline** designed to extract data from external APIs and relational databases, validate and standardize datasets, store raw data, and generate analytics-ready data models.

The pipeline supports:

- REST API ingestion
- PostgreSQL database extraction
- Incremental data loading
- Data validation and normalization
- BigQuery data warehouse loading
- SQL-based warehouse transformations
- Automated workflow scheduling using Prefect

The goal is to create a reliable and scalable data platform for analytics and reporting.

---

# 💡 Business Problem

Modern organizations store data across multiple systems:

- SaaS platforms
- Transaction databases
- External APIs

This creates challenges:

- Data fragmentation
- Duplicate records
- Manual reporting processes
- Inconsistent business metrics

This pipeline solves these challenges by creating a centralized data workflow or Data pipeline to bring in data from different sources :

```
Multiple Sources
        |
        v
Validated Pipeline
        |
        v
Analytics Warehouse
```

---

# 🏗️ Architecture

```
                 Data Sources

          REST APIs       PostgreSQL
              |              |
              +--------------+

                    |
                    v

              Extract Layer

        API Connectors
        Database Connectors


                    |
                    v

            Validation Layer

        Pydantic Models
        Data Quality Checks


                    |
                    v

              Raw Storage

        Raw API Data
        Raw Database Data


                    |
                    v

              Load Layer

        PostgreSQL
        BigQuery Warehouse


                    |
                    v

          Transformation Layer

        Python Transformations
        BigQuery SQL Models


                    |
                    v

          Analytics Ready Data
```

---

# ⚙️ Key Features

## 🔄 Multi-Source Data Extraction

Supports:

- REST API extraction
- PostgreSQL extraction
- Incremental extraction
- Cursor-based API pagination
- Database-based incremental loading


---

## 🧹 Data Validation

Using Pydantic:

- Schema validation
- Data type checking
- Required field validation
- Data normalization
- Data quality rules


---

## 🔄 Transformation Layer

Supports two transformation approaches:

### Python Transformations

Used for:

- API normalization
- Complex business logic
- Data preparation


### BigQuery SQL Transformations

Used for:

- Warehouse modelling
- Analytics tables
- Business reporting datasets


---

# 🗄️ Storage Architecture

## Bronze Layer (Raw)

Stores original extracted data:

```
Raw API Data

Raw Database Data
```

Purpose:

- Data recovery
- Pipeline replay
- Historical preservation


## Analytics Layer

Produces:

```
Clean Business Tables

Analytics Models

Reporting Datasets
```

---

# ⏱️ Automation & Scheduling

Pipeline orchestration is handled using Prefect.

Workflow:

```
Prefect Scheduler

        |
        v

Extract API Data

        |
        v

Extract Database Data

        |
        v

Validate Data

        |
        v

Save Raw Data

        |
        v

Load BigQuery

        |
        v

Run SQL Transformations

        |
        v

Analytics Tables
```

---

# 📂 Project Structure

```
app/
 └── main.py
        → Pipeline entry point


extract/
        → API and database connectors


validate/
        → Schema validation and data quality checks


transform/
        → Python transformation logic


load_data/
        → Database and BigQuery loaders


save_raw/
        → Raw data storage layer


sql/
        → BigQuery SQL transformation models


tests/
        → Pipeline tests


prefect.yaml
        → Workflow scheduling configuration
```

---

# 🔐 Security

Security practices:

- Secrets stored using environment variables
- Cloud credentials managed securely
- No hardcoded API keys
- Protected database connections


---

# 🧰 Technology Stack

## Programming

- Python


## Data Processing

- Pandas
- PyArrow


## Databases

- PostgreSQL
- Google BigQuery


## Data Engineering

- Prefect
- SQLAlchemy
- Pydantic
- HTTPX


## DevOps

- Docker
- GitHub Actions


---

# 📊 Pipeline Outputs

The pipeline produces:

- Clean structured datasets
- Analytics-ready warehouse tables
- Standardized business models
- BI-compatible reporting datasets


---

# 🎯 Use Cases

This architecture can support:

- Business intelligence pipelines
- SaaS analytics platforms
- Financial data processing
- E-commerce analytics
- Operational reporting systems


---

# 🚀 Execution Modes

## Scheduled Execution

Automated runs using Prefect schedules.

Example:

```
Daily pipeline execution
```

---

## Incremental Execution

Only new or updated records are processed.

Examples:

```
API cursor tracking

Database updated_at extraction
```

---

# 🔮 Future Improvements

Planned improvements:

- Data observability layer
- Pipeline monitoring
- Automated data quality reporting
- dbt transformation models
- Streaming ingestion using Kafka/PubSub
- Cloud-native deployment with Kubernetes


---

# 🚀 Getting Started

## Clone Repository

```bash
git clone https://github.com/nathan6012/Multi_Source_ETLT-.git
```

## Create Virtual Environment

```bash
python -m venv venv
```

Activate:

Linux/Mac:

```bash
source venv/bin/activate
```

Windows:

```bash
venv\Scripts\activate
```

## Install Dependencies

```bash
pip install -r requirements.txt
```

## Configure Credentials

Set:

- Database credentials
- API keys
- Google Cloud credentials

Use environment variables.

---

## Run Pipeline

```bash
python -m app.main
```

or navigate through the project modules.

---

# 👤 Author

**Shamola Nassan**

Data Engineering | ETL Systems | Automation Pipelines