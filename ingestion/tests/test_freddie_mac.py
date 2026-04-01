import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from ingestion.sources.freddie_mac import FreddieReader
from ingestion.models.loan import LoanRecord, LoanPerformanceRecord


@pytest.fixture
def reader(tmp_path):
    """Crea un FreddieReader apuntando a una carpeta temporal"""
    return FreddieReader(data_path=str(tmp_path), chunk_size=10)


@pytest.fixture
def sample_origination_chunk():
    """Simula un chunk del fichero de originación con 3 registros"""
    return pd.DataFrame([
        {
            "loan_id": "L001",
            "origination_date": "01/2018",
            "original_upb": "250000",
            "original_ltv": "80",
            "original_interest_rate": "4.5",
            "original_loan_term": "360",
            "number_of_borrowers": "2",
            "credit_score": "720",
            "property_state": "CA",
            "property_type": "SF",
            "loan_purpose": "P",
            "seller_name": "Bank of America",
            "servicer_name": "Bank of America",
            "first_payment_date": None,
            "first_time_homebuyer_flag": None,
            "maturity_date": None,
            "msa": None, "mip": None,
            "number_of_units": None,
            "occupancy_status": None,
            "original_cltv": None,
            "original_dti": None,
            "zip_code": None,
            "primary_mortgage_insurance_percent": None,
            "product_type": None,
            "number_of_units_2": None,
            "occupancy_status_2": None,
            "conforming_flag": None
        },
        {
            "loan_id": "L002",
            "origination_date": "02/2018",
            "original_upb": "180000",
            "original_ltv": "75",
            "original_interest_rate": "3.8",
            "original_loan_term": "360",
            "number_of_borrowers": "1",
            "credit_score": None,  # credit score nulo, debe aceptarse
            "property_state": "TX",
            "property_type": "SF",
            "loan_purpose": "R",
            "seller_name": "Wells Fargo",
            "servicer_name": "Wells Fargo",
            "first_payment_date": None, "first_time_homebuyer_flag": None,
            "maturity_date": None, "msa": None, "mip": None,
            "number_of_units": None, "occupancy_status": None,
            "original_cltv": None, "original_dti": None, "zip_code": None,
            "primary_mortgage_insurance_percent": None, "product_type": None,
            "number_of_units_2": None, "occupancy_status_2": None,
            "conforming_flag": None
        },
        {
            "loan_id": "L003",
            "origination_date": "03/2018",
            "original_upb": "300000",
            "original_ltv": "-5",  # LTV negativo, debe descartarse
            "original_interest_rate": "5.0",
            "original_loan_term": "360",
            "number_of_borrowers": "1",
            "credit_score": "680",
            "property_state": "FL",
            "property_type": "SF",
            "loan_purpose": "P",
            "seller_name": "Chase",
            "servicer_name": "Chase",
            "first_payment_date": None, "first_time_homebuyer_flag": None,
            "maturity_date": None, "msa": None, "mip": None,
            "number_of_units": None, "occupancy_status": None,
            "original_cltv": None, "original_dti": None, "zip_code": None,
            "primary_mortgage_insurance_percent": None, "product_type": None,
            "number_of_units_2": None, "occupancy_status_2": None,
            "conforming_flag": None
        }
    ])


def test_reader_valid_records(reader, sample_origination_chunk):
    """De 3 registros, 2 son válidos y 1 se descarta por LTV negativo"""
    with patch("pandas.read_csv", return_value=iter([sample_origination_chunk])):
        chunks = list(reader.read_origination("test_file.txt"))

    assert len(chunks) == 1
    assert len(chunks[0]) == 2  # L001 y L002, L003 descartado


def test_reader_returns_loan_records(reader, sample_origination_chunk):
    """Los registros devueltos son instancias de LoanRecord"""
    with patch("pandas.read_csv", return_value=iter([sample_origination_chunk])):
        chunks = list(reader.read_origination("test_file.txt"))

    for record in chunks[0]:
        assert isinstance(record, LoanRecord)


def test_reader_credit_score_null_accepted(reader, sample_origination_chunk):
    """El registro L002 con credit score nulo debe ser aceptado"""
    with patch("pandas.read_csv", return_value=iter([sample_origination_chunk])):
        chunks = list(reader.read_origination("test_file.txt"))

    loan_ids = [r.loan_id for r in chunks[0]]
    assert "L002" in loan_ids


def test_reader_invalid_ltv_discarded(reader, sample_origination_chunk):
    """El registro L003 con LTV negativo debe ser descartado"""
    with patch("pandas.read_csv", return_value=iter([sample_origination_chunk])):
        chunks = list(reader.read_origination("test_file.txt"))

    loan_ids = [r.loan_id for r in chunks[0]]
    assert "L003" not in loan_ids


def test_reader_empty_chunk(reader):
    """Un chunk vacío devuelve una lista vacía"""
    empty_chunk = pd.DataFrame(columns=["loan_id"])
    with patch("pandas.read_csv", return_value=iter([empty_chunk])):
        chunks = list(reader.read_origination("test_file.txt"))

    assert chunks[0] == []