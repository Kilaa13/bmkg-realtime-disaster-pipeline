# BMKG Real-Time Disaster Data Pipeline

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=Snowflake&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=Python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=Docker&logoColor=white)
![Telegram](https://img.shields.io/badge/Telegram-26A5E4?style=for-the-badge&logo=Telegram&logoColor=white)

An end-to-end Automated Data Engineering Pipeline that ingests, cleanses, transforms, and loads real-time earthquake and disaster data from **BMKG (Badan Meteorologi, Klimatologi, dan Geofisika)** into **Snowflake Data Warehouse**, orchestrated by **Apache Airflow**, integrated with **Telegram Real-time Alerting System**, and visualised on **Looker Studio**.

---

## Architecture Overview

```
[BMKG Open API] 
       │
       ▼
[Apache Airflow DAG] ──(Ingestion & Cleansing)──► [Raw/Silver Data]
       │                                                 │
       ├─► [Telegram Bot Alert]                          ▼
       │   (Real-time Failure/Notification)       [Snowflake Warehouse]
       │                                                 │
       └─────────────────────────────────────────► [Looker Studio Dashboard]
```
## Key Features
- Automated Data Ingestion: Scheduled hourly ingestion of real-time seismic data directly from BMKG API endpoints.
- Data Cleansing & Transformation (Medallion Architecture):
- Raw Layer: JSON responses collected and stored.
- Silver Layer: Cleaned and parsed geospatial formats (Latitude, Longitude, Magnitude, Depth).
- Gold/KPI Layer: Aggregated metrics built for fast business intelligence queries.
- Modern Data Warehouse (Snowflake): Structured schema design (BMKG_DB.SILVER & BMKG_DB.KPI) for high-performance querying.
- Automated Failure Alerting: Custom Telegram Bot (@bmkg_pipeline_bot) integration providing immediate notification on DAG/task execution failures.
- Geospatial & KPI Dashboard: Executive-ready Looker Studio report featuring interactive earthquake heatmaps, magnitude trends, and hazard depth distributions.

## Tech Stack
- Orchestration: Apache Airflow
- Data Warehouse: Snowflake
- Language: Python 3.10+ 
- Containerization: Docker & Docker Compose
- Alerting: Telegram Bot API
- Visualization: Google Looker Studio

## Repository Structure
```
bmkg-realtime-disaster-pipeline/
├── dags/
│   └── bmkg_disaster_pipeline.py    # Definisi Airflow DAG & Penjadwalan Orchestration
├── scripts/
│   ├── __init__.py                  # Inisialisasi package modul Python
│   ├── bronze_bmkg.py               # Extract data mentah dari API BMKG (Bronze Layer)
│   ├── silver_bmkg.py               # Cleansing & transformasi data koordinat/tipe data (Silver Layer)
│   ├── gold_bmkg.py                 # Agregasi data & Pembuatan Metrik Bisnis/KPI (Gold Layer)
│   ├── load_snowflake.py            # Pembuatan koneksi & Pemuatan Data ke Snowflake Warehouse
│   └── notifications.py             # Alerting System Notifikasi Telegram Bot
├── .env.example                     # Template Variabel Lingkungan / Environment Variables
├── .gitignore                       # Daftar file/folder yang diabaikan oleh Git
├── README.md                        # Dokumentasi Proyek
├── docker-compose.yaml              # Konfigurasi Lingkungan Airflow (Docker Container)
└── requirements.txt                 # Library Python (Pandas, Snowflake-Connector, Requests, dll)
```
## Getting Started
1. Prerequisites
- Docker Desktop installed
- Snowflake Account set up
- Telegram Bot Token & Chat ID

2. Environment Configuration
Clone this repository and create a .env file based on .env.example:
```
git clone [https://github.com/Kilaa13/bmkg-realtime-disaster-pipeline.git](https://github.com/Kilaa13/bmkg-realtime-disaster-pipeline.git)
cd bmkg-realtime-disaster-pipeline
cp .env.example .env
```
3. Run Pipeline with Docker
Start the Apache Airflow environment:
```
docker-compose up -d
```
Access the Airflow Webserver at http://localhost:8080 (Default login: airflow / airflow).

📊 Dashboard Preview
Check out the live interactive dashboard on [![Looker Studio](https://img.shields.io/badge/Looker_Studio-Live_Dashboard-4285F4?style=for-the-badge&logo=google&logoColor=white)](https://lookerstudio.google.com/reporting/bafc6771-47d9-423f-af61-b29db24110db)
- Geospatial Map: Real-time earthquake distribution across Indonesia.
- Magnitude Distribution: Categorized view of minor, moderate, and major seismic events.
- Executive Scorecard: Key aggregates on total events, max magnitude recorded, and average depth.
