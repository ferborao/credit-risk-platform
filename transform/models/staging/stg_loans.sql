-- Staging: vista limpia sobre Silver
-- No hay transformaciones aquí, solo renombrar y seleccionar columnas relevantes

with source as (
    select * from parquet.`/home/fernando/credit-risk-platform/data/silver/loans_parquet`
),

staged as (
    select
        loan_id,
        credit_score,
        original_upb,
        original_ltv,
        original_interest_rate,
        original_loan_term,
        number_of_borrowers,
        original_dti,
        property_state,
        loan_purpose,
        property_type,
        seller_name,
        servicer_name,
        first_payment_date,
        _ingestion_timestamp,
        _silver_timestamp
    from source
)

select * from staged