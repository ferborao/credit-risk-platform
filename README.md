# Credit Risk Data Platform

End-to-end data pipeline for mortgage portfolio credit risk analysis, built on real Freddie Mac Single Family Loan-Level Dataset.

## Description

This platform processes 200,000 real mortgage loans and implements a full Medallion architecture (Bronze → Silver → Gold) to compute credit risk metrics such as vintage analysis, geographic concentration, and risk profiles by LTV.

## Technology stack

| Layer | Technology |
|---|---|
| Ingestion | Python, PySpark |
| Processing | PySpark, Delta Lake |
| Transformation | dbt |
| Orchestration | Apache Airflow |
| Infrastructure | Terraform, Docker |
| Visualization | Streamlit, Plotly |
| CI/CD | GitHub Actions |

## Architecture

```
Freddie Mac (real data)
        ↓
Ingestion Python + PySpark — read and apply schema
        ↓
Bronze — Delta Lake — 200,000 raw records
        ↓
Silver — PySpark — 199,867 cleaned records
        ↓
Gold — dbt — risk metrics
        ↓
Streamlit Dashboard
```

## Calculated metrics

- **Vintage analysis** — cumulative default rate by quarterly cohort. Cohorts from 2006–2008 show visible deterioration compared to earlier periods.
- **Geographic concentration** — percentage of total UPB by state. California concentrates over 25% of the exposure.
- **Risk profile by LTV** — distribution of loans and volumes by LTV buckets (60, 70, 80, 90, 90+).

## Project structure

```
credit-risk-platform/
├── pipelines/
│   ├── bronze/         # Ingest to Delta Lake
│   ├── silver/         # Cleaning and quality checks
│   └── gold/           # Export Gold to Parquet
├── transform/          # dbt models (staging + marts)
├── orchestration/      # Airflow DAGs
├── dashboard/          # Streamlit app
└── infra/              # Terraform (Azure)
```

## Tests

```bash
pytest pipelines/tests/ -v    # 10 tests
cd transform && dbt test       # 15 dbt tests
```

## How to run

### Requirements
- Python 3.12+
- Java 11+ (for PySpark)
- WSL2 / Linux / macOS

### Installation

```bash
git clone https://github.com/ferborao/credit-risk-platform
cd credit-risk-platform
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Full pipeline

```bash
# 1. Download Freddie Mac data to data/raw/freddie_mac/
#    https://freddiemac.embs.com/FLoan/secure/auth.php

# 2. Bronze
python pipelines/bronze/ingest_freddie_mac.py

# 3. Silver
python pipelines/silver/transform_loans.py

# 4. Gold
cd transform
SILVER_PATH=$(pwd)/../data/silver dbt run
cd ..
GOLD_PATH=$(pwd)/data/gold python pipelines/export_gold.py

# 5. Dashboard
streamlit run dashboard/app.py
```

### Airflow (automated orchestration)

```bash
export AIRFLOW_HOME=~/airflow
airflow db migrate
airflow users create --username admin --role Admin --email admin@admin.com --password admin --firstname Admin --lastname Admin
cp orchestration/credit_risk_dag.py ~/airflow/dags/
airflow webserver --port 8080 &
airflow scheduler
```

## Databricks version

This project has also been migrated to Databricks Free Edition, using:
- PySpark notebooks for Bronze and Silver, writing to Unity Catalog
- dbt-databricks for the Gold layer (same SQL models as the local version)
- Databricks Workflows orchestrating the full pipeline (Bronze → Silver → dbt run/test)

**Catalog:** `credit_risk_platform` with schemas `bronze`, `silver`, `gold`

## Data source

Freddie Mac Single Family Loan-Level Dataset — real loan-level origination and performance data since 1999. Registration on freddiemac.com is required to download the data.
