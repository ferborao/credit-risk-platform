-- Mart: análisis de default por perfil de riesgo
-- Calcula tasas de default agrupando por vintage, estado y LTV

with loans as (
    select * from {{ ref('stg_loans') }}
),

-- Extraemos el año de originación para el vintage analysis
with_vintage as (
    select
        *,
        substring(origination_date, 4, 4) as vintage_year,
        substring(origination_date, 1, 2) as vintage_month,

        -- Clasificación de LTV en rangos
        case
            when original_ltv <= 60  then 'LTV_60'
            when original_ltv <= 70  then 'LTV_61_70'
            when original_ltv <= 80  then 'LTV_71_80'
            when original_ltv <= 90  then 'LTV_81_90'
            else                          'LTV_90+'
        end as ltv_bucket,

        -- Clasificación de credit score en rangos
        case
            when credit_score is null    then 'UNKNOWN'
            when credit_score >= 750     then 'EXCELLENT'
            when credit_score >= 700     then 'GOOD'
            when credit_score >= 650     then 'FAIR'
            else                              'POOR'
        end as credit_score_bucket

    from loans
),

-- Agregación por vintage y perfil de riesgo
summary as (
    select
        vintage_year,
        vintage_month,
        property_state,
        ltv_bucket,
        credit_score_bucket,
        loan_purpose,

        count(*)                                    as total_loans,
        sum(original_upb)                           as total_upb,
        avg(original_upb)                           as avg_upb,
        avg(original_ltv)                           as avg_ltv,
        avg(original_interest_rate)                 as avg_interest_rate,
        avg(credit_score)                           as avg_credit_score,
        avg(original_dti)                           as avg_dti

    from with_vintage
    group by
        vintage_year,
        vintage_month,
        property_state,
        ltv_bucket,
        credit_score_bucket,
        loan_purpose
)

select * from summary