# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Freddie Mac Origination Data
# MAGIC Ingesta raw de datos de originación de préstamos hipotecarios.
# MAGIC Los datos se escriben sin transformar en formato Delta Lake.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)
from delta import configure_spark_with_delta_pip
import os

# COMMAND ----------

# Inicializar Spark con soporte Delta Lake
builder = (
    SparkSession.builder
    .appName("credit-risk-bronze-ingestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"Spark version: {spark.version}")

# COMMAND ----------

# Schema del fichero de originación de Freddie Mac
# Definimos los tipos explícitamente — nunca inferir schemas en producción
ORIGINATION_SCHEMA = StructType([
    StructField("credit_score",                         StringType(), True),
    StructField("first_payment_date",                   StringType(), True),
    StructField("first_time_homebuyer_flag",            StringType(), True),
    StructField("maturity_date",                        StringType(), True),
    StructField("msa",                                  StringType(), True),
    StructField("mip",                                  StringType(), True),
    StructField("number_of_units",                      StringType(), True),
    StructField("occupancy_status",                     StringType(), True),
    StructField("original_cltv",                        StringType(), True),
    StructField("original_dti",                         StringType(), True),
    StructField("original_upb",                         StringType(), True),
    StructField("original_ltv",                         StringType(), True),
    StructField("original_interest_rate",               StringType(), True),
    StructField("channel",                              StringType(), True),
    StructField("ppm_flag",                             StringType(), True),
    StructField("product_type",                         StringType(), True),
    StructField("property_state",                       StringType(), True),
    StructField("property_type",                        StringType(), True),
    StructField("zip_code",                             StringType(), True),
    StructField("loan_id",                              StringType(), True),
    StructField("loan_purpose",                         StringType(), True),
    StructField("original_loan_term",                   StringType(), True),
    StructField("number_of_borrowers",                  StringType(), True),
    StructField("seller_name",                          StringType(), True),
    StructField("servicer_name",                        StringType(), True),
    StructField("super_conforming_flag",                StringType(), True),
    StructField("pre_harp_loan_id",                     StringType(), True),
])

# COMMAND ----------

# Rutas
DATA_RAW_PATH = os.getenv("DATA_RAW_PATH", "./data/raw/freddie_mac")
BRONZE_PATH = os.getenv("BRONZE_PATH", "./data/bronze")

# COMMAND ----------

def ingest_origination(input_path: str, output_path: str) -> None:
    """
    Lee el fichero de originación y lo escribe en Bronze como Delta.
    Bronze = datos crudos, append-only, sin transformar.
    """
    print(f"Leyendo datos desde: {input_path}")

    df = spark.read.format("csv") \
        .option("sep", "|") \
        .option("header", "false") \
        .option("nullValue", "") \
        .schema(ORIGINATION_SCHEMA) \
        .load(input_path)

    record_count = df.count()
    print(f"Registros leídos: {record_count:,}")

    # Añadimos metadatos de auditoría
    from pyspark.sql.functions import current_timestamp, lit, input_file_name
    df = df \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_source_file", input_file_name())

    # Escribimos en Delta — modo append para no sobreescribir datos anteriores
    df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("first_payment_date") \
        .save(f"{output_path}/origination")

    print(f"Datos escritos en Bronze: {output_path}/origination")
    print(f"Particionado por: first_payment_date")


# COMMAND ----------

if __name__ == "__main__":
    import glob
    # Busca todos los ficheros de originación
    orig_pattern = f"{DATA_RAW_PATH}/sample_orig_*.txt"
    ingest_origination(orig_pattern, BRONZE_PATH)