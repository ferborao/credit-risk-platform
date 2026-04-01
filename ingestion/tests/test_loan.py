import pytest
from pydantic import ValidationError
from ingestion.models.loan import LoanRecord, LoanPerformanceRecord


# ── Fixtures: registros válidos que reutilizamos en varios tests ──

@pytest.fixture
def valid_loan_data():
    return {
        "loan_id": "ABC123",
        "origination_date": "01/2018",
        "original_upb": 250000.0,
        "original_ltv": 80.0,
        "original_interest_rate": 4.5,
        "original_loan_term": 360,
        "number_of_borrowers": 2,
        "credit_score": 720.0,
        "property_state": "CA",
        "property_type": "SF",
        "loan_purpose": "P",
        "seller_name": "Bank of America",
        "servicer_name": "Bank of America"
    }

@pytest.fixture
def valid_performance_data():
    return {
        "loan_id": "ABC123",
        "monthly_reporting_period": "01/2019",
        "current_upb": 245000.0,
        "loan_age": 12,
        "current_loan_delinquency_status": "0",
        "modification_flag": "N",
        "zero_balance_code": None
    }


# ── Tests de LoanRecord ──

def test_loan_record_valid(valid_loan_data):
    """Un registro con datos correctos se crea sin errores"""
    record = LoanRecord(**valid_loan_data)
    assert record.loan_id == "ABC123"
    assert record.original_ltv == 80.0

def test_loan_record_credit_score_optional(valid_loan_data):
    """El credit score puede ser None"""
    valid_loan_data["credit_score"] = None
    record = LoanRecord(**valid_loan_data)
    assert record.credit_score is None

def test_loan_record_ltv_negative_rejected(valid_loan_data):
    """Un LTV negativo debe ser rechazado"""
    valid_loan_data["original_ltv"] = -10.0
    with pytest.raises(ValidationError):
        LoanRecord(**valid_loan_data)

def test_loan_record_ltv_zero_rejected(valid_loan_data):
    """Un LTV de cero debe ser rechazado"""
    valid_loan_data["original_ltv"] = 0.0
    with pytest.raises(ValidationError):
        LoanRecord(**valid_loan_data)

def test_loan_record_interest_rate_zero_rejected(valid_loan_data):
    """Un tipo de interés de cero debe ser rechazado"""
    valid_loan_data["original_interest_rate"] = 0.0
    with pytest.raises(ValidationError):
        LoanRecord(**valid_loan_data)

def test_loan_record_credit_score_out_of_range(valid_loan_data):
    """Un credit score fuera del rango 300-850 debe ser rechazado"""
    valid_loan_data["credit_score"] = 900.0
    with pytest.raises(ValidationError):
        LoanRecord(**valid_loan_data)

def test_loan_record_credit_score_below_minimum(valid_loan_data):
    """Un credit score por debajo de 300 debe ser rechazado"""
    valid_loan_data["credit_score"] = 200.0
    with pytest.raises(ValidationError):
        LoanRecord(**valid_loan_data)


# ── Tests de LoanPerformanceRecord ──

def test_performance_record_valid(valid_performance_data):
    """Un registro de rendimiento válido se crea correctamente"""
    record = LoanPerformanceRecord(**valid_performance_data)
    assert record.loan_id == "ABC123"
    assert record.is_default is False

def test_performance_record_not_default(valid_performance_data):
    """Estado 0 = al día, no es default"""
    valid_performance_data["current_loan_delinquency_status"] = "0"
    record = LoanPerformanceRecord(**valid_performance_data)
    assert record.is_default is False

def test_performance_record_default_at_3_months(valid_performance_data):
    """Estado 3 = 90 días de mora = default"""
    valid_performance_data["current_loan_delinquency_status"] = "3"
    record = LoanPerformanceRecord(**valid_performance_data)
    assert record.is_default is True

def test_performance_record_default_xx(valid_performance_data):
    """Estado XX = foreclosure = default"""
    valid_performance_data["current_loan_delinquency_status"] = "XX"
    record = LoanPerformanceRecord(**valid_performance_data)
    assert record.is_default is True

def test_performance_record_2_months_not_default(valid_performance_data):
    """Estado 2 = 60 días de mora, todavía no es default"""
    valid_performance_data["current_loan_delinquency_status"] = "2"
    record = LoanPerformanceRecord(**valid_performance_data)
    assert record.is_default is False