import pytest
import shutil
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder
        .appName("test-silver")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def bronze_delta(tmp_path, spark):
    """Crea una tabla Delta Bronze sintética para los tests"""
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql.functions import current_timestamp, lit

    schema = StructType([
        StructField("loan_id",                   StringType(), True),
        StructField("origination_date",          StringType(), True),
        StructField("credit_score",              StringType(), True),
        StructField("original_upb",              StringType(), True),
        StructField("original_ltv",              StringType(), True),
        StructField("original_interest_rate",    StringType(), True),
        StructField("original_loan_term",        StringType(), True),
        StructField("number_of_borrowers",       StringType(), True),
        StructField("original_dti",              StringType(), True),
        StructField("property_state",            StringType(), True),
        StructField("loan_purpose",              StringType(), True),
        StructField("property_type",             StringType(), True),
        StructField("first_payment_date",        StringType(), True),
        StructField("first_time_homebuyer_flag", StringType(), True),
        StructField("maturity_date",             StringType(), True),
        StructField("msa",                       StringType(), True),
        StructField("mip",                       StringType(), True),
        StructField("number_of_units",           StringType(), True),
        StructField("occupancy_status",          StringType(), True),
        StructField("original_cltv",             StringType(), True),
        StructField("zip_code",                  StringType(), True),
        StructField("primary_mortgage_insurance_percent", StringType(), True),
        StructField("product_type",              StringType(), True),
        StructField("seller_name",               StringType(), True),
        StructField("servicer_name",             StringType(), True),
        StructField("number_of_units_2",         StringType(), True),
        StructField("occupancy_status_2",        StringType(), True),
        StructField("conforming_flag",           StringType(), True),
        StructField("_ingestion_timestamp",      StringType(), True),
        StructField("_source_file",              StringType(), True),
    ])

    data = [
        # Registro válido con credit score
        ("L001", "01/2018", "720", "250000", "80", "4.5", "360", "2", "35", "ca", "p", "sf",
         None, None, None, None, None, None, None, None, None, None, None,
         "Bank of America", "Bank of America", None, None, None, "2024-01-01", "file1.txt"),
        # Registro válido sin credit score
        ("L002", "02/2018", None, "180000", "75", "3.8", "360", "1", "40", "tx", "r", "sf",
         None, None, None, None, None, None, None, None, None, None, None,
         "Wells Fargo", "Wells Fargo", None, None, None, "2024-01-01", "file1.txt"),
        # Registro inválido — LTV negativo, debe ser filtrado en Silver
        ("L003", "03/2018", "680", "300000", "-5", "5.0", "360", "1", "45", "fl", "p", "sf",
         None, None, None, None, None, None, None, None, None, None, None,
         "Chase", "Chase", None, None, None, "2024-01-01", "file1.txt"),
        # Registro inválido — credit score fuera de rango
        ("L004", "04/2018", "900", "200000", "85", "4.0", "360", "1", "38", "ny", "p", "sf",
         None, None, None, None, None, None, None, None, None, None, None,
         "Citi", "Citi", None, None, None, "2024-01-01", "file1.txt"),
    ]

    bronze_path = str(tmp_path / "bronze")
    df = spark.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").save(f"{bronze_path}/origination")
    return bronze_path


@pytest.fixture
def output_path(tmp_path):
    path = str(tmp_path / "silver")
    yield path
    if os.path.exists(path):
        shutil.rmtree(path)


def test_silver_filters_invalid_ltv(spark, bronze_delta, output_path):
    """L003 con LTV negativo debe ser filtrado en Silver"""
    from pipelines.silver.transform_loans import transform_loans
    transform_loans(bronze_delta, output_path)

    df = spark.read.format("delta").load(f"{output_path}/loans")
    loan_ids = [r.loan_id for r in df.select("loan_id").collect()]
    assert "L003" not in loan_ids


def test_silver_filters_invalid_credit_score(spark, bronze_delta, output_path):
    """L004 con credit score 900 debe ser filtrado en Silver"""
    from pipelines.silver.transform_loans import transform_loans
    transform_loans(bronze_delta, output_path)

    df = spark.read.format("delta").load(f"{output_path}/loans")
    loan_ids = [r.loan_id for r in df.select("loan_id").collect()]
    assert "L004" not in loan_ids


def test_silver_valid_records_pass(spark, bronze_delta, output_path):
    """L001 y L002 son válidos y deben estar en Silver"""
    from pipelines.silver.transform_loans import transform_loans
    transform_loans(bronze_delta, output_path)

    df = spark.read.format("delta").load(f"{output_path}/loans")
    loan_ids = [r.loan_id for r in df.select("loan_id").collect()]
    assert "L001" in loan_ids
    assert "L002" in loan_ids


def test_silver_types_are_cast(spark, bronze_delta, output_path):
    """Los tipos deben ser numéricos en Silver, no strings"""
    from pipelines.silver.transform_loans import transform_loans
    transform_loans(bronze_delta, output_path)

    df = spark.read.format("delta").load(f"{output_path}/loans")
    assert str(df.schema["original_ltv"].dataType) == "DoubleType()"
    assert str(df.schema["original_upb"].dataType) == "DoubleType()"
    assert str(df.schema["original_loan_term"].dataType) == "IntegerType()"


def test_silver_has_audit_column(spark, bronze_delta, output_path):
    """Silver debe tener columna de auditoría _silver_timestamp"""
    from pipelines.silver.transform_loans import transform_loans
    transform_loans(bronze_delta, output_path)

    df = spark.read.format("delta").load(f"{output_path}/loans")
    assert "_silver_timestamp" in df.columns


def test_silver_state_is_uppercase(spark, bronze_delta, output_path):
    """property_state debe estar en mayúsculas en Silver"""
    from pipelines.silver.transform_loans import transform_loans
    transform_loans(bronze_delta, output_path)

    df = spark.read.format("delta").load(f"{output_path}/loans")
    states = [r.property_state for r in df.select("property_state").collect()]
    assert all(s == s.upper() for s in states)