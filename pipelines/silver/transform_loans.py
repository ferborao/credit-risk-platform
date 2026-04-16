# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Loan Origination
# MAGIC Limpieza, tipado y validación de datos de originación.
# MAGIC Bronze → Silver: de strings crudos a tipos correctos con calidad garantizada.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, upper, when, current_timestamp,
    to_date, regexp_replace
)
from pyspark.sql.types import DoubleType, IntegerType
from delta import configure_spark_with_delta_pip
import os

# COMMAND ----------

builder = (
    SparkSession.builder
    .appName("credit-risk-silver-loans")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.ansi.enabled", "false")  # Añade esta línea
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# COMMAND ----------

BRONZE_PATH = os.getenv("BRONZE_PATH", "./data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "./data/silver")

# COMMAND ----------

def clean_loans(df: DataFrame) -> DataFrame:
    """
    Transforma el DataFrame Bronze en Silver aplicando:
    - Casting de tipos correctos
    - Limpieza de strings
    - Filtrado de registros inválidos
    - Columnas de auditoría
    """

    df_typed = df \
        .withColumn("credit_score",            col("credit_score").cast(DoubleType())) \
        .withColumn("original_upb",            col("original_upb").cast(DoubleType())) \
        .withColumn("original_ltv",            col("original_ltv").cast(DoubleType())) \
        .withColumn("original_interest_rate",  col("original_interest_rate").cast(DoubleType())) \
        .withColumn("original_loan_term",      col("original_loan_term").cast(IntegerType())) \
        .withColumn("number_of_borrowers",     col("number_of_borrowers").cast(IntegerType())) \
        .withColumn("original_dti",            col("original_dti").cast(DoubleType())) \
        .withColumn("property_state",          upper(trim(col("property_state")))) \
        .withColumn("loan_purpose",            upper(trim(col("loan_purpose")))) \
        .withColumn("property_type",           upper(trim(col("property_type"))))

    # Filtrar registros inválidos — estas reglas son las mismas que en Pydantic
    # pero aplicadas a escala con Spark
    df_clean = df_typed \
        .filter(col("loan_id").isNotNull()) \
        .filter(col("original_upb").isNotNull() & (col("original_upb") > 0)) \
        .filter(col("original_ltv").isNotNull() & (col("original_ltv") > 0)) \
        .filter(col("original_interest_rate").isNotNull() & (col("original_interest_rate") > 0)) \
        .filter(
            col("credit_score").isNull() |
            ((col("credit_score") >= 300) & (col("credit_score") <= 850))
        )

    # Añadir columna de auditoría Silver
    df_clean = df_clean.withColumn("_silver_timestamp", current_timestamp())

    return df_clean


# COMMAND ----------

def run_quality_checks(df: DataFrame) -> None:
    """
    Validaciones de calidad del dato post-transformación.
    Si algo falla aquí, el pipeline se detiene antes de escribir.
    """
    total = df.count()

    checks = {
        "sin loan_id nulos":           df.filter(col("loan_id").isNull()).count() == 0,
        "LTV siempre positivo":        df.filter(col("original_ltv") <= 0).count() == 0,
        "tipo interés siempre positivo": df.filter(col("original_interest_rate") <= 0).count() == 0,
        "credit score en rango válido": df.filter(
            col("credit_score").isNotNull() &
            ((col("credit_score") < 300) | (col("credit_score") > 850))
        ).count() == 0,
    }

    failed = [check for check, passed in checks.items() if not passed]

    if failed:
        raise ValueError(f"Quality checks fallidos: {failed}")

    print(f"Quality checks OK — {total:,} registros válidos")


# COMMAND ----------

def transform_loans(bronze_path: str, silver_path: str) -> None:
    print(f"Leyendo Bronze desde: {bronze_path}")

    df_bronze = spark.read.format("delta").load(f"{bronze_path}/origination")
    print(f"Registros en Bronze: {df_bronze.count():,}")

    df_silver = clean_loans(df_bronze)

    run_quality_checks(df_silver)

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("property_state") \
        .save(f"{silver_path}/loans")

    print(f"Silver escrito en: {silver_path}/loans")
    print(f"Particionado por: property_state")

    # Exportar también en Parquet para consumo de dbt
    df_silver.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(f"{silver_path}/loans_parquet")

    print(f"Parquet exportado en: {silver_path}/loans_parquet")

# COMMAND ----------

if __name__ == "__main__":
    transform_loans(BRONZE_PATH, SILVER_PATH)