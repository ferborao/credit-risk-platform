import pytest
import os
import shutil
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark():
    """Crea una sesión Spark compartida para todos los tests"""
    builder = (
        SparkSession.builder
        .appName("test-bronze")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")  # Reducir para tests
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_csv(tmp_path):
    """Crea un fichero CSV sintético con formato Freddie Mac"""
    # Orden: credit_score|first_payment_date|first_time_homebuyer_flag|maturity_date|
    # msa|mip|number_of_units|occupancy_status|original_cltv|original_dti|
    # original_upb|original_ltv|original_interest_rate|channel|ppm_flag|
    # product_type|property_state|property_type|zip_code|loan_id|
    # loan_purpose|original_loan_term|number_of_borrowers|seller_name|servicer_name|
    # super_conforming_flag|pre_harp_loan_id
    content = (
        "720|01/2018|N|01/2048|12345|0|1|P|80|35|250000|80|4.5|R|N|FRM|CA|SF|90210|L001|P|360|2|Bank of America|Bank of America|N|\n"
        "680|02/2018|N|02/2048|12346|0|1|P|75|40|180000|75|3.8|R|N|FRM|TX|SF|75001|L002|R|360|1|Wells Fargo|Wells Fargo|N|\n"
        "|03/2018|N|03/2048|12347|0|1|P|90|45|300000|90|5.0|R|N|FRM|FL|SF|33101|L003|P|360|1|Chase|Chase|N|\n"
    )
    csv_file = tmp_path / "sample_origination.txt"
    csv_file.write_text(content)
    return str(tmp_path)


@pytest.fixture
def output_path(tmp_path):
    """Carpeta temporal para la salida Bronze"""
    path = str(tmp_path / "bronze")
    yield path
    if os.path.exists(path):
        shutil.rmtree(path)


def test_bronze_creates_delta_table(spark, sample_csv, output_path):
    """La ingesta crea una tabla Delta en la ruta de salida"""
    import sys
    sys.path.insert(0, ".")
    from pipelines.bronze.ingest_freddie_mac import ingest_origination

    ingest_origination(sample_csv, output_path)

    df = spark.read.format("delta").load(f"{output_path}/origination")
    assert df.count() == 3


def test_bronze_has_audit_columns(spark, sample_csv, output_path):
    """Los datos Bronze tienen columnas de auditoría"""
    from pipelines.bronze.ingest_freddie_mac import ingest_origination

    ingest_origination(sample_csv, output_path)

    df = spark.read.format("delta").load(f"{output_path}/origination")
    assert "_ingestion_timestamp" in df.columns
    assert "_source_file" in df.columns


def test_bronze_preserves_raw_data(spark, sample_csv, output_path):
    """Bronze no transforma los datos, los guarda tal cual vienen"""
    from pipelines.bronze.ingest_freddie_mac import ingest_origination

    ingest_origination(sample_csv, output_path)

    df = spark.read.format("delta").load(f"{output_path}/origination")
    loan_ids = [row.loan_id for row in df.select("loan_id").collect()]
    assert "L001" in loan_ids
    assert "L002" in loan_ids
    assert "L003" in loan_ids


def test_bronze_null_credit_score_preserved(spark, sample_csv, output_path):
    """Bronze preserva los nulos sin rechazarlos — eso es trabajo de Silver"""
    from pipelines.bronze.ingest_freddie_mac import ingest_origination

    ingest_origination(sample_csv, output_path)

    df = spark.read.format("delta").load(f"{output_path}/origination")
    nulls = df.filter(df.credit_score.isNull()).count()
    assert nulls == 1  # L003 tiene credit_score nulo
