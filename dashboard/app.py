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
    """Carga datos reales del mart de vintage analysis"""
    path = "/home/fernando/credit-risk-platform/data/silver/loans_parquet"
    df = pd.read_parquet(path, columns=[
        "loan_id", "first_payment_date", "original_ltv",
        "original_upb", "original_interest_rate", "credit_score",
        "property_state", "loan_purpose", "original_dti"
    ])
    df["vintage_year"] = df["first_payment_date"].str[3:7].astype(int)
    df["vintage_month"] = df["first_payment_date"].str[0:2].astype(int)
    df["cohort_quarter"] = ((df["vintage_month"] - 1) // 3 + 1).astype(int)
    df["cohort"] = df["vintage_year"].astype(str) + "-Q" + df["cohort_quarter"].astype(str)
    df["ltv_bucket"] = pd.cut(
        df["original_ltv"],
        bins=[0, 60, 70, 80, 90, 999],
        labels=["LTV_60", "LTV_61_70", "LTV_71_80", "LTV_81_90", "LTV_90+"]
    )
    return df


@st.cache_data
def load_portfolio_data():
    """Carga datos reales de concentración geográfica"""
    path = "/home/fernando/credit-risk-platform/data/silver/loans_parquet"
    df = pd.read_parquet(path, columns=[
        "property_state", "original_upb", "original_ltv",
        "original_interest_rate", "credit_score", "loan_id"
    ])
    return df.groupby("property_state").agg(
        total_loans=("loan_id", "count"),
        total_upb=("original_upb", "sum"),
        avg_ltv=("original_ltv", "mean"),
        avg_interest_rate=("original_interest_rate", "mean"),
        avg_credit_score=("credit_score", "mean")
    ).reset_index().sort_values("total_upb", ascending=False).head(10)


@st.cache_data
def load_risk_profile_data():
    """Carga datos reales de perfil de riesgo por LTV"""
    path = "/home/fernando/credit-risk-platform/data/silver/loans_parquet"
    df = pd.read_parquet(path, columns=["original_ltv", "original_upb", "loan_id"])
    df["ltv_bucket"] = pd.cut(
        df["original_ltv"],
        bins=[0, 60, 70, 80, 90, 999],
        labels=["LTV_60", "LTV_61_70", "LTV_71_80", "LTV_81_90", "LTV_90+"]
    )
    return df.groupby("ltv_bucket", observed=True).agg(
        total_loans=("loan_id", "count"),
        avg_upb=("original_upb", "mean")
    ).reset_index()


# ── Sidebar ──────────────────────────────────────────────────────────────────
available_years = sorted(load_vintage_data()["vintage_year"].unique().tolist())
vintage_years = st.sidebar.multiselect(
    "Años de cohorte",
    options=available_years,
    default=available_years
)

# ── KPIs principales ─────────────────────────────────────────────────────────
df_all = load_vintage_data()
total_loans = f"{len(df_all):,}"
total_upb = f"${df_all['original_upb'].sum() / 1e9:.1f}B"
avg_ltv = f"{df_all['original_ltv'].mean():.1f}%"

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total préstamos", total_loans)
with col2:
    st.metric("UPB Total", total_upb)
with col3:
    st.metric("LTV Medio", avg_ltv)
with col4:
    st.metric("Años disponibles", "2006-2008, 2018")

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