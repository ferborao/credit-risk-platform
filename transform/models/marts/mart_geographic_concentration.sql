-- Mart: concentración geográfica de riesgo
-- Calcula qué porcentaje del riesgo total está concentrado en cada estado
-- Un banco con demasiado riesgo en un estado es vulnerable a shocks locales

with loans as (
    select * from {{ ref('stg_loans') }}
),

state_summary as (
    select
        property_state,

        count(*)                as total_loans,
        sum(original_upb)       as total_upb,
        avg(original_ltv)       as avg_ltv,
        avg(original_interest_rate) as avg_interest_rate,
        avg(credit_score)       as avg_credit_score

    from loans
    group by property_state
),

-- Calculamos el porcentaje de concentración sobre el total de la cartera
with_concentration as (
    select
        property_state,
        total_loans,
        total_upb,
        avg_ltv,
        avg_interest_rate,
        avg_credit_score,

        -- Porcentaje de préstamos sobre el total
        total_loans / sum(total_loans) over ()           as loan_count_pct,

        -- Porcentaje de UPB sobre el total — esta es la métrica clave
        total_upb / sum(total_upb) over ()               as upb_concentration_pct,

        -- Ranking por concentración
        rank() over (order by total_upb desc)            as concentration_rank

    from state_summary
)

select * from with_concentration
order by concentration_rank