import streamlit as st
import pandas as pd
from databricks import sql
import os
from dotenv import load_dotenv
import plotly.express as px

load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
HTTP_PATH = os.getenv("HTTP_PATH")

@st.cache_data(ttl=60)
def load_data(query):
    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    ) as conn:
        return pd.read_sql(query, conn)

st.title("📊 Gold Price Analytics Dashboard")

# Load tables
monthly_df = load_data("SELECT * FROM default.gold_monthly_trend")
volatility_df = load_data("SELECT * FROM default.gold_volatility")

# MONTHLY KPIs
monthly_df = monthly_df.sort_values("month")

latest_month = monthly_df.iloc[-1]
prev_month = monthly_df.iloc[-2]

mom_growth = (
    (latest_month["monthly_avg_price"] - prev_month["monthly_avg_price"]) /
    prev_month["monthly_avg_price"] * 100
)

col1, col2, col3 = st.columns(3)
col1.metric("Latest Monthly Avg Price", f"${latest_month['monthly_avg_price']:.2f}")
col2.metric("MoM Growth", f"{mom_growth:.2f}%")
col3.metric("Monthly Volume", f"{latest_month['monthly_volume']:.0f}")

# VOLATILITY KPIs

volatility_index = volatility_df["daily_price_change"].std()
max_spike = volatility_df["daily_price_change"].abs().max()

col4, col5 = st.columns(2)
col4.metric("Volatility Index (STD)", f"{volatility_index:.2f}")
col5.metric("Max Daily Spike", f"${max_spike:.2f}")

# Charts
fig1 = px.line(
    monthly_df,
    x="month",
    y="monthly_avg_price",
    title="Monthly Average Gold Price"
)
st.plotly_chart(fig1, use_container_width=True)

fig2 = px.bar(
    monthly_df,
    x="month",
    y="monthly_volume",
    title="Monthly Trading Volume"
)
st.plotly_chart(fig2, use_container_width=True)
