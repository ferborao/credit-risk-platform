import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import os

# ── Configuración de la página ──────────────────────────────────────────────
st.set_page_config(
    page_title="Credit Risk Platform",
    page_icon="🏦",
    layout="wide",
)

st.title("🏦 Credit Risk Data Platform")
st.markdown("Análisis de riesgo de cartera hipotecaria — Freddie Mac Dataset")

# ── Datos sintéticos para demo ───────────────────────────────────────────────
# Cuando tengamos los datos reales de Freddie Mac,
# sustituiremos esto por lecturas de las tablas Delta

@st.cache_data
def load_vintage_data():
    """Datos sintéticos de vintage analysis"""
    data = []
    import random
    random.seed(42)
    for year in range(2003, 2012):
        for quarter in range(1, 5):
            cohort = f"{year}-Q{quarter}"
            # Las cohortes de 2006-2008 tienen peor rendimiento (burbuja)
            base_default = 0.02
            if 2006 <= year <= 2008:
                base_default = 0.08 + (year - 2006) * 0.03
            for month in range(1, 37):
                default_rate = base_default * (1 - 0.97**month)
                data.append({
                    "cohort": cohort,
                    "cohort_year": year,
                    "cohort_quarter": quarter,
                    "loan_age_months": month,
                    "cumulative_default_rate": round(default_rate * 100, 3),
                })
    return pd.DataFrame(data)


@st.cache_data
def load_portfolio_data():
    """Datos sintéticos de composición de cartera"""
    return pd.DataFrame({
        "property_state": ["CA", "TX", "FL", "NY", "IL", "OH", "PA", "GA", "NC", "MI"],
        "total_loans":    [45000, 32000, 28000, 22000, 18000, 15000, 14000, 12000, 11000, 10000],
        "total_upb":      [18.5, 9.2, 7.8, 11.3, 4.2, 3.1, 3.5, 2.8, 2.4, 2.1],
        "avg_ltv":        [78.2, 75.1, 79.8, 71.3, 76.5, 74.2, 73.8, 77.1, 76.3, 75.9],
        "avg_credit_score": [712, 718, 705, 724, 715, 710, 716, 708, 711, 713],
    })


@st.cache_data
def load_risk_profile_data():
    """Datos sintéticos de perfil de riesgo"""
    return pd.DataFrame({
        "ltv_bucket":    ["LTV_60", "LTV_61_70", "LTV_71_80", "LTV_81_90", "LTV_90+"],
        "total_loans":   [52000, 68000, 95000, 72000, 31000],
        "default_rate":  [1.2, 2.1, 3.4, 5.8, 9.2],
        "avg_upb":       [185000, 210000, 245000, 268000, 295000],
    })


# ── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.header("Filtros")
vintage_years = st.sidebar.multiselect(
    "Años de cohorte",
    options=list(range(2003, 2012)),
    default=[2004, 2006, 2007, 2008, 2010]
)

# ── KPIs principales ─────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total préstamos", "318,000", "+12% YoY")
with col2:
    st.metric("UPB Total", "$64.9B", "-3.2% YoY")
with col3:
    st.metric("Default Rate", "3.8%", "+0.4pp")
with col4:
    st.metric("LTV Medio", "76.2%", "-0.8pp")

st.divider()

# ── Vintage Analysis ─────────────────────────────────────────────────────────
st.subheader("📈 Vintage Analysis — Tasa de default acumulada por cohorte")

df_vintage = load_vintage_data()
df_filtered = df_vintage[df_vintage["cohort_year"].isin(vintage_years)]

# Una línea por cohorte anual
df_annual = df_filtered.groupby(
    ["cohort_year", "loan_age_months"]
)["cumulative_default_rate"].mean().reset_index()
df_annual["cohort_year"] = df_annual["cohort_year"].astype(str)

fig_vintage = px.line(
    df_annual,
    x="loan_age_months",
    y="cumulative_default_rate",
    color="cohort_year",
    title="Tasa de default acumulada por año de originación",
    labels={
        "loan_age_months": "Edad del préstamo (meses)",
        "cumulative_default_rate": "Default rate acumulado (%)",
        "cohort_year": "Año"
    },
    color_discrete_sequence=px.colors.qualitative.Set2
)
fig_vintage.update_layout(hovermode="x unified")
st.plotly_chart(fig_vintage, use_container_width=True)

st.caption(
    "Las cohortes de 2006-2008 muestran tasas de default significativamente superiores, "
    "reflejo de los estándares de concesión laxos durante la burbuja inmobiliaria."
)

st.divider()

# ── Concentración Geográfica ─────────────────────────────────────────────────
col_map, col_bar = st.columns([1.5, 1])

with col_map:
    st.subheader("🗺️ Concentración geográfica del riesgo")
    df_geo = load_portfolio_data()

    fig_map = px.choropleth(
        df_geo,
        locations="property_state",
        locationmode="USA-states",
        color="total_upb",
        scope="usa",
        color_continuous_scale="Reds",
        labels={"total_upb": "UPB ($B)"},
        title="Distribución del UPB por estado"
    )
    fig_map.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig_map, use_container_width=True)

with col_bar:
    st.subheader("🏆 Top 10 estados por volumen")
    df_geo_sorted = df_geo.sort_values("total_upb", ascending=True)
    fig_bar = px.bar(
        df_geo_sorted,
        x="total_upb",
        y="property_state",
        orientation="h",
        labels={"total_upb": "UPB ($B)", "property_state": "Estado"},
        color="total_upb",
        color_continuous_scale="Reds",
    )
    fig_bar.update_layout(showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# ── Perfil de riesgo por LTV ─────────────────────────────────────────────────
st.subheader("⚠️ Default rate por bucket de LTV")

df_risk = load_risk_profile_data()

col_r1, col_r2 = st.columns(2)

with col_r1:
    fig_default = px.bar(
        df_risk,
        x="ltv_bucket",
        y="default_rate",
        color="default_rate",
        color_continuous_scale="RdYlGn_r",
        labels={"ltv_bucket": "Bucket LTV", "default_rate": "Default rate (%)"},
        title="Default rate por rango de LTV"
    )
    st.plotly_chart(fig_default, use_container_width=True)

with col_r2:
    fig_volume = px.bar(
        df_risk,
        x="ltv_bucket",
        y="total_loans",
        color="total_loans",
        color_continuous_scale="Blues",
        labels={"ltv_bucket": "Bucket LTV", "total_loans": "Nº préstamos"},
        title="Volumen de préstamos por rango de LTV"
    )
    st.plotly_chart(fig_volume, use_container_width=True)

st.divider()

# ── Footer ───────────────────────────────────────────────────────────────────
st.caption(
    "📊 Credit Risk Data Platform · Datos: Freddie Mac Single Family Loan-Level Dataset · "
    "Stack: Python · PySpark · Delta Lake · dbt · Airflow · Streamlit"
)