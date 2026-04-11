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
    content = (
        "720|01/2018|N|01/2048|12345|0|1|P|80|80|2|35|250000|360|01/2018|P|SF|1|P|CA|90210|0|FRM|4.5|Bank of America|Bank of America|L001|Y\n"
        "680|02/2018|N|02/2048|12346|0|1|P|75|75|1|40|180000|360|02/2018|R|SF|1|P|TX|75001|0|FRM|3.8|Wells Fargo|Wells Fargo|L002|Y\n"
        "|03/2018|N|03/2048|12347|0|1|P|90|90|1|45|300000|360|03/2018|P|SF|1|P|FL|33101|0|FRM|5.0|Chase|Chase|L003|Y\n"
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
