import pandas as pd
import logging
from pathlib import Path
from typing import Iterator
from ingestion.models.loan import LoanRecord, LoanPerformanceRecord

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Columnas del fichero de originación de Freddie Mac (orden fijo en el CSV)
ORIGINATION_COLUMNS = [
    "credit_score", "first_payment_date", "first_time_homebuyer_flag",
    "maturity_date", "msa", "mip", "number_of_units", "occupancy_status",
    "original_ltv", "original_cltv", "number_of_borrowers", "original_dti",
    "original_upb", "original_loan_term", "origination_date", "loan_purpose",
    "property_type", "number_of_units_2", "occupancy_status_2", "property_state",
    "zip_code", "primary_mortgage_insurance_percent", "product_type",
    "original_interest_rate", "seller_name", "servicer_name", "loan_id",
    "conforming_flag"
]

# Columnas del fichero de rendimiento mensual
PERFORMANCE_COLUMNS = [
    "loan_id", "monthly_reporting_period", "servicer_name", "current_interest_rate",
    "current_upb", "loan_age", "remaining_months_to_maturity", "adjusted_remaining_months",
    "maturity_date", "msa", "current_loan_delinquency_status", "modification_flag",
    "zero_balance_code", "zero_balance_effective_date", "last_paid_installment_date",
    "foreclosure_date", "disposition_date", "foreclosure_costs", "prop_preservation_repair_costs",
    "asset_recovery_costs", "misc_holding_expenses", "taxes_insurance",
    "net_sale_proceeds", "credit_enhancement_proceeds", "repurchase_make_whole_proceeds",
    "other_foreclosure_proceeds", "non_interest_bearing_upb", "principal_forgiveness_upb"
]


class FreddieReader:
    """
    Lee los ficheros de Freddie Mac y los convierte en objetos validados.
    Procesa en chunks para no cargar todo en memoria de golpe.
    """

    def __init__(self, data_path: str, chunk_size: int = 50_000):
        self.data_path = Path(data_path)
        self.chunk_size = chunk_size

    def read_origination(self, filename: str) -> Iterator[list[LoanRecord]]:
        """
        Lee el fichero de originación en chunks y devuelve listas de LoanRecord.
        Los registros que no pasen validación se descartan y se loguean.
        """
        filepath = self.data_path / filename
        logger.info(f"Leyendo fichero de originación: {filepath}")

        chunks_processed = 0
        records_valid = 0
        records_invalid = 0

        for chunk in pd.read_csv(
            filepath,
            sep="|",
            header=None,
            names=ORIGINATION_COLUMNS,
            chunksize=self.chunk_size,
            dtype=str,  # Leemos todo como string, Pydantic hace la conversión
            na_values=["", " "]
        ):
            valid_records = []

            for _, row in chunk.iterrows():
                try:
                    record = LoanRecord(
                        loan_id=str(row["loan_id"]),
                        origination_date=str(row["origination_date"]),
                        original_upb=float(row["original_upb"]),
                        original_ltv=float(row["original_ltv"]),
                        original_interest_rate=float(row["original_interest_rate"]),
                        original_loan_term=int(row["original_loan_term"]),
                        number_of_borrowers=int(row["number_of_borrowers"]),
                        credit_score=float(row["credit_score"]) if pd.notna(row["credit_score"]) else None,
                        property_state=str(row["property_state"]),
                        property_type=str(row["property_type"]),
                        loan_purpose=str(row["loan_purpose"]),
                        seller_name=str(row["seller_name"]),
                        servicer_name=str(row["servicer_name"])
                    )
                    valid_records.append(record)
                    records_valid += 1

                except Exception as e:
                    records_invalid += 1
                    logger.debug(f"Registro inválido descartado: {e}")

            chunks_processed += 1
            logger.info(
                f"Chunk {chunks_processed} procesado — "
                f"válidos: {records_valid}, descartados: {records_invalid}"
            )

            yield valid_records

    def read_performance(self, filename: str) -> Iterator[list[LoanPerformanceRecord]]:
        """
        Lee el fichero de rendimiento mensual en chunks.
        """
        filepath = self.data_path / filename
        logger.info(f"Leyendo fichero de rendimiento: {filepath}")

        for chunk in pd.read_csv(
            filepath,
            sep="|",
            header=None,
            names=PERFORMANCE_COLUMNS,
            chunksize=self.chunk_size,
            dtype=str,
            na_values=["", " "]
        ):
            valid_records = []

            for _, row in chunk.iterrows():
                try:
                    record = LoanPerformanceRecord(
                        loan_id=str(row["loan_id"]),
                        monthly_reporting_period=str(row["monthly_reporting_period"]),
                        current_upb=float(row["current_upb"]) if pd.notna(row["current_upb"]) else None,
                        loan_age=int(row["loan_age"]) if pd.notna(row["loan_age"]) else None,
                        current_loan_delinquency_status=str(row["current_loan_delinquency_status"]),
                        modification_flag=str(row["modification_flag"]),
                        zero_balance_code=str(row["zero_balance_code"]) if pd.notna(row["zero_balance_code"]) else None
                    )
                    valid_records.append(record)

                except Exception as e:
                    logger.debug(f"Registro de rendimiento inválido: {e}")

            yield valid_records