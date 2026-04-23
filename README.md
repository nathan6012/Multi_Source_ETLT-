Python](https://img.shields.io/badge/Python-3.x-blue)
![Prefect](https://img.shields.io/badge/Orchestration-Prefect-06b6d4)
![Pandas](https://img.shields.io/badge/Data-Pandas-black)
![SQLAlchemy](https://img.shields.io/badge/Database-SQLAlchemy-red)
![Neon PostgreSQL](https://img.shields.io/badge/Storage-Neon%20PostgreSQL-00E599?logo=postgresql&logoColor=white)
![Pydantic](https://img.shields.io/badge/Validation-Pydantic-009688?logo=pydantic&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/Automation-GitHub%20Actions-2088FF?logo=githubactions&logoColor=white)


# ETL Data Engineering Pipeline System

A production-style multi-source ETL pipeline that extracts, validates, transforms, and loads data from multiple sources into a centralized analytics-ready structure.
can use Postgres Or Data Warehouse like Big Query 
---

🚀 Overview

This system automates end-to-end data workflows from:

- 📡 APIs
- 🗄️ Databases
- 📄 Excel files
- 📁 Local file uploads

It processes raw unclean / structured data into a clean, validated, and analytics-ready format stored in a structured data lake and database layer.

---

🧠 Business Value

This pipeline solves a core business problem:

«Organizations struggle with fragmented data across APIs, databases, and files.»

This system provides:

- A single source of truth
- Automated data ingestion
- Standardized data cleaning and validation
- BI-ready structured datasets
- Scheduled + event-driven processing

---

⚙️ Key Features

🔄 Multi-Source Data Ingestion

- API data extraction
- Database replication
- Excel file ingestion (local uploads supported)

🧹 Data Processing Layer

- Data validation using schema models
- Data normalization
- Deduplication and cleaning

🔄 Transformation Layer

- Business logic transformations
- Structured modeling for analytics
- BI-ready dataset preparation

🗄️ Data Storage

- Raw data storage (data lake)
- Staging database tables
- Processed analytics tables

⏱️ Automation

- Scheduled execution (cron-based)
- File-based triggers (local folder monitoring via GitHub Actions workflow)

---

🏗️ Architecture

Extract Layer
   ├── API
   ├── Database
   └── Excel Files

        ↓

Validation Layer
   ├── Schema validation
   ├── Data quality checks

        ↓

Transform Layer
   ├── Business logic
   ├── Data modeling

        ↓

Load Layer
   ├── Data Lake (raw storage)
   ├── Staging DB
   └── Analytics-ready tables

---

📂 Project Structure

app/              → Orchestration & entry point
extract/          → Data source connectors (API, DB, files)
validate/         → Schema validation & data quality rules
transform/        → Business logic + data modeling
load_data/        → Database & storage loaders
save_raw/         → Raw data persistence layer
local/            → Local file ingestion (Excel uploads)
datalake/         → Raw + processed data storage

---

⚡ Execution Modes

🔹 Scheduled Execution

Runs automatically using GitHub Actions cron schedule for periodic data syncing.

🔹 Event-Driven Execution

Triggers when new Excel files are added to the "local/" directory.

---

🔐 Security

- Credentials managed via environment variables and GitHub Secrets
- No hardcoded API keys or database credentials

---

📈 Output

The pipeline produces:

- Clean structured datasets
- Analytics-ready tables
- Standardized data models
- BI-compatible outputs for dashboards and reporting

---

🧩 Tech Stack

- Python
- Prefect (orchestration)
- GitHub Actions (CI/CD automation)
- SQL Databases / PostgresSql neon 
- Pandas
- sqlalchemy
- pathlib
- httpx
- openpyxl
- pydantic 


---

🎯 Use Cases

- Business analytics pipelines
- SaaS reporting systems
- Financial data aggregation
- E-commerce data consolidation
- Operational dashboards

---

📌 Future Improvements

- Cloud data warehouse integration (BigQuery / Snowflake)
- code optimization
- tests 
- Real-time streaming ingestion
- Airbyte connector integration
- Dashboard layer (Streamlit / Metabase)
- Observability (logging + monitoring system)

---
🚀 Usage
1. Clone the repository
git clone https://github.com/nathan6012/Multi_Source_ETLT-.git
cd your-repo
2. Create virtual environment (optional but recommended)
python -m venv venv

source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows

3. Install dependencies
pip install -r requirments.txt
4. Run the pipeline in 
python -m app.main

👨‍💻 Author:shamola_Nassan 
Built as a scalable data engineering pipeline for multi-source data integration and automation.
