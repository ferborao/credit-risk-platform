from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import shutil

builder = (
    SparkSession.builder
    .appName("export-gold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

GOLD_PATH = os.getenv("GOLD_PATH", "./data/gold")
SPARK_WAREHOUSE = os.path.expanduser(
    "~/credit-risk-platform/transform/spark-warehouse"
)

tables = [
    "mart_default_analysis",
    "mart_vintage_analysis",
    "mart_geographic_concentration",
]

for table in tables:
    src = f"{SPARK_WAREHOUSE}/{table}"
    dst = f"{GOLD_PATH}/{table}"
    print(f"Exportando {table}...")
    df = spark.read.parquet(src)
    print(f"  Registros: {df.count():,}")
    df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(dst)
    print(f"  Guardado en: {dst}")

print("Export Gold completado.")
spark.stop()