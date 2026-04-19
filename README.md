# Credit Risk Data Platform

Pipeline de datos end-to-end para análisis de riesgo de cartera hipotecaria, construido sobre datos reales de Freddie Mac Single Family Loan-Level Dataset.

## Descripción

Plataforma que procesa 200,000 hipotecas reales e implementa una arquitectura Medallion completa (Bronze → Silver → Gold) para calcular métricas de riesgo de crédito: vintage analysis, concentración geográfica y análisis de perfil por LTV.

## Stack tecnológico

| Capa | Tecnología |
|---|---|
| Ingesta | Python, PySpark |
| Procesamiento | PySpark, Delta Lake |
| Transformación | dbt |
| Orquestación | Apache Airflow |
| Infraestructura | Terraform, Docker |
| Visualización | Streamlit, Plotly |
| CI/CD | GitHub Actions |

## Arquitectura

```
Freddie Mac (datos reales)
        ↓
Ingesta Python + PySpark — lectura y schema
        ↓
Bronze — Delta Lake — 200,000 registros crudos
        ↓
Silver — PySpark — 199,867 registros limpios
        ↓
Gold — dbt — métricas de riesgo
        ↓
Dashboard Streamlit
```

## Métricas calculadas

- **Vintage analysis** — tasa de default acumulada por cohorte trimestral. Las cohortes de 2006-2008 muestran deterioro visible frente a periodos anteriores.
- **Concentración geográfica** — porcentaje del UPB total por estado. California concentra más del 25% del riesgo.
- **Perfil de riesgo por LTV** — distribución de préstamos y volumen por bucket de LTV (60, 70, 80, 90, 90+).

## Estructura del proyecto

```
credit-risk-platform/
├── pipelines/
│   ├── bronze/         # Ingesta a Delta Lake
│   ├── silver/         # Limpieza y quality checks
│   └── gold/           # Export Gold a Parquet
├── transform/          # Modelos dbt (staging + marts)
├── orchestration/      # DAG de Airflow
├── dashboard/          # Aplicación Streamlit
└── infra/              # Terraform (Azure)
```

## Tests

```bash
pytest pipelines/tests/ -v    # 10 tests
cd transform && dbt test       # 15 tests dbt
```

## Cómo ejecutar

### Requisitos
- Python 3.12+
- Java 11+ (para PySpark)
- WSL2 / Linux / macOS

### Instalación

```bash
git clone https://github.com/ferborao/credit-risk-platform
cd credit-risk-platform
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Pipeline completo

```bash
# 1. Descargar datos de Freddie Mac en data/raw/freddie_mac/
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

### Airflow (orquestación automática)

```bash
export AIRFLOW_HOME=~/airflow
airflow db migrate
airflow users create --username admin --role Admin --email admin@admin.com --password admin --firstname Admin --lastname Admin
cp orchestration/credit_risk_dag.py ~/airflow/dags/
airflow webserver --port 8080 &
airflow scheduler
```

## Fuente de datos

Freddie Mac Single Family Loan-Level Dataset — datos reales de originación y rendimiento de hipotecas desde 1999. Requiere registro gratuito en freddiemac.com.