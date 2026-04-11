-- Mart: vintage analysis
-- Agrupa préstamos por cohorte (trimestre de originación)
-- para analizar cómo evoluciona el riesgo por generación de préstamos

with loans as (
    select * from {{ ref('stg_loans') }}
),

with_cohort as (
    select
        *,
        -- Construimos el identificador de cohorte: año + trimestre
        concat(
            substring(origination_date, 4, 4),
            '-Q',
            cast(
                ceil(cast(substring(origination_date, 1, 2) as int) / 3.0)
                as int
            )
        ) as cohort,

        -- Año y trimestre por separado para ordenación
        cast(substring(origination_date, 4, 4) as int) as cohort_year,
        cast(
            ceil(cast(substring(origination_date, 1, 2) as int) / 3.0)
            as int
        ) as cohort_quarter,

        -- Bucket de LTV
        case
            when original_ltv <= 60  then 'LTV_60'
            when original_ltv <= 70  then 'LTV_61_70'
            when original_ltv <= 80  then 'LTV_71_80'
            when original_ltv <= 90  then 'LTV_81_90'
            else                          'LTV_90+'
        end as ltv_bucket,

        -- Bucket de credit score
        case
            when credit_score is null then 'UNKNOWN'
            when credit_score >= 750  then 'EXCELLENT'
            when credit_score >= 700  then 'GOOD'
            when credit_score >= 650  then 'FAIR'
            else                          'POOR'
        end as credit_score_bucket

    from loans
),

cohort_summary as (
    select
        cohort,
        cohort_year,
        cohort_quarter,
        ltv_bucket,
        credit_score_bucket,
        property_state,

        count(*)            as total_loans,
        sum(original_upb)   as total_upb,
        avg(original_upb)   as avg_upb,
        avg(original_ltv)   as avg_ltv,
        avg(original_interest_rate) as avg_interest_rate,
        avg(credit_score)   as avg_credit_score,
        avg(original_dti)   as avg_dti,

        -- Concentración de UPB en LTV alto (proxy de riesgo)
        sum(case when original_ltv > 80 then original_upb else 0 end)
            / nullif(sum(original_upb), 0) as high_ltv_upb_pct

    from with_cohort
    group by
        cohort,
        cohort_year,
        cohort_quarter,
        ltv_bucket,
        credit_score_bucket,
        property_state
)

select * from cohort_summary
order by cohort_year, cohort_quarter