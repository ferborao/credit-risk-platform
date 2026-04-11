-- Staging: vista limpia sobre Silver
-- No hay transformaciones aquí, solo renombrar y seleccionar columnas relevantes

with source as (
    select * from delta.`{{ env_var('SILVER_PATH', './data/silver') }}/loans`
),

staged as (
    select
        loan_id,
        origination_date,
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
        _ingestion_timestamp,
        _silver_timestamp
    from source
)

select * from staged