## ETL Data Engineering Pipeline System

![Prefect](https://img.shields.io/badge/Orchestration-Prefect-06b6d4)
![Storage](https://img.shields.io/badge/Data%20Lake-S3%20Compatible-007ACC?logo=amazons3&logoColor=white)
![Pandas](https://img.shields.io/badge/Data-Pandas-black)
![SQLAlchemy](https://img.shields.io/badge/Database-SQLAlchemy-red)
![Neon PostgreSQL](https://img.shields.io/badge/Storage-Neon%20PostgreSQL-00E599?logo=postgresql&logoColor=white)
![Pydantic](https://img.shields.io/badge/Validation-Pydantic-009688?logo=pydantic&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/Automation-GitHub%20Actions-2088FF?logo=githubactions&logoColor=white)
![BigQuery](https://img.shields.io/badge/Data%20Warehouse-BigQuery-4285F4?logo=googlecloud&logoColor=white)

---

## 📌 Overview

This project is a **production-style multi-source ETL pipeline** designed to extract, validate, transform, and load data from multiple heterogeneous sources into structured, analytics-ready storage systems.

It supports:

- APIs  
- Databases Postgres and Big Query 
- Excel files  
- Local file ingestion  

All data  from Business multi sources transformed ,modeled and standardized into a unified model/location  for analytics and reporting.

---

## 💡 Business Value

Modern organizations struggle with fragmented data across systems.

This pipeline solves that by providing:

- A **single source of truth**
- Automated and scheduled data ingestion across all sources 
- Standardized local datalakes validation and transformation layers
- BI-ready structured datasets in Database and Big Query warehouse 
- Scalable multi-source integration

---

## ⚙️ Key Features

### 🔄 Multi-Source Ingestion
- REST API extraction
- Database replication (Postgres / external sources)
- Excel file ingestion (local uploads supported)

### 🧹 Data Processing Layer
- Schema validation using Pydantic
- Data normalization
- Deduplication and cleaning

### 🔄 Transformation Layer
- Business logic transformations
- Structured data modeling
- Analytics-ready dataset generation

### 🗄️ Data Storage Layer
- S3/R2 Cloud  data lake storage
- Staging database tables
- Final analytics-ready tables (Postgres / BigQuery)

### ⏱️ Automation & Scheduling
- Cron-based scheduling via Prefect
- File-based triggers for local ingestion
- CI/CD automation via GitHub Actions

---

## 🏗️ Architecture

Extract Layer
 ├── API
 ├── Database
 └── Excel Files

        ↓

Validation Layer
 ├── Schema validation (Pydantic)
 ├── Data quality checks

        ↓

Transformation Layer
 ├── Business logic
 ├── Data modeling

        ↓

Load Layer
 ├── Data Lake (raw storage)
 ├── PostgreSQL (staging)
 └── BigQuery (analytics)

---

## 📂 Project Structure

app/          → Orchestration & entry point  
extract/      → Data source connectors (API, DB, files)  
validate/     → Schema validation & quality rules  
transform/    → Business logic & transformations  
load_data/    → Database & warehouse loaders  
save_raw/     →  S3/R2 Datalake
local/        → Local file ingestion (Excel uploads)  
datalake/     → R2 processed data storage  

---

## 🔐 Security

- Secrets managed via environment variables & Prefect Secret Blocks  
- No hardcoded credentials  
- API keys, DB URLs, and GCP credentials stored securely  

---

## 📊 Outputs

The pipeline produces:

- Clean structured datasets  
- Analytics-ready tables  
- Standardized business models  
- BI-compatible outputs for dashboards and reporting  

---

## 🧰 Tech Stack

- Python  
- Prefect (workflow orchestration)  
- PostgreSQL (Neon)  
- Google BigQuery  
- Pandas  
- boto3
- SQLAlchemy  
- Pydantic  
- HTTPX  
- OpenPyXL  
- PyArrow  
- GitHub Actions (CI/CD)

---

## 🎯 Use Cases

- Business intelligence pipelines  
- SaaS reporting systems  
- Financial data aggregation  
- E-commerce analytics  
- Operational dashboards  

---

## 🚀 Execution Modes

### Scheduled Runs
Automated execution using Prefect cron schedules.

### Event-Driven Runs
Triggered by new file uploads in local ingestion directory.

---

## 🔮 Future Improvements

- Real-time streaming ingestion (Kafka / PubSub)  
- Data observability & monitoring layer  
- Airbyte connector integration  
- Dashboard layer (Streamlit / Metabase)  
- Advanced testing framework (pytest + data validation tests)  
- Cloud-native deployment (Docker + Kubernetes)

---

## 🚀 Getting Started


python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate    

# make sure to get all creditentials 

### 1. Clone repository
```bash
git clone https://github.com/nathan6012/Multi_Source_ETLT-.git

explore the code and project Structure { focus on app/main}

pip install -r requirements.txt

python -m app.main or navigate around the project folder in your System 



Shamola Nassan
Data Engineering | ETL Systems | Automation Pipelines
