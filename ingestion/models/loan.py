from pydantic import BaseModel, field_validator
from typing import Optional
from datetime import date

class LoanRecord(BaseModel):
    """
    Schema de validación para registros de préstamos de Freddie Mac.
    Pydantic valida tipos y constraints automáticamente.
    """

    loan_id: str
    origination_date: str
    original_upb: float          # Unpaid Balance - importe original del préstamo
    original_ltv: float          # Loan-to-Value ratio
    original_interest_rate: float
    original_loan_term: int      # En meses
    number_of_borrowers: int
    credit_score: Optional[float] = None  # Puede ser nulo en Freddie Mac
    property_state: str
    property_type: str
    loan_purpose: str            # Purchase, Refinance, etc.
    seller_name: str
    servicer_name: str

    @field_validator('original_ltv')
    @classmethod
    def ltv_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError(f'LTV debe ser positivo, recibido: {v}')
        return v

    @field_validator('original_interest_rate')
    @classmethod
    def rate_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError(f'Tipo de interés debe ser positivo, recibido: {v}')
        return v

    @field_validator('credit_score')
    @classmethod
    def credit_score_range(cls, v):
        if v is not None and not (300 <= v <= 850):
            raise ValueError(f'Credit score fuera de rango (300-850): {v}')
        return v
    
class LoanPerformanceRecord(BaseModel):
    """
    Schema para datos de rendimiento mensual del préstamo.
    Estos registros llegan separados en Freddie Mac.
    """
    loan_id: str
    monthly_reporting_period: str
    current_upb: Optional[float] = None
    loan_age: Optional[int] = None       # Meses desde originación
    current_loan_delinquency_status: str  # 0, 1, 2... meses de mora, o XX para default
    modification_flag: str
    zero_balance_code: Optional[str] = None  # Razón por la que el préstamo se cerró

    @property
    def is_default(self) -> bool:
        """Un préstamo está en default si tiene 90+ días de mora (código 3 o superior)"""
        try:
            return int(self.current_loan_delinquency_status) >= 3
        except (ValueError, TypeError):
            return self.current_loan_delinquency_status == 'XX'