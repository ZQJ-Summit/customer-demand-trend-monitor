import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(
    page_title="Customer Demand Trend Monitor",
    layout="wide"
)

st.title("📈 Customer Demand Trend Monitor")
st.write("Daily monitoring for customer demand & product trends.")

# -----------------------------------------
# Load Data
# -----------------------------------------
uploaded_file = st.sidebar.file_uploader("Upload today's CSV file", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)

    # Ensure Ship Date exists
    if "Ship Date" not in df.columns:
        st.error("Your file does not contain 'Ship Date' column. Please verify the file format.")
        st.stop()

    # Convert date
    df["Ship Date"] = pd.to_datetime(df["Ship Date"], errors="coerce")
    df["Date"] = df["Ship Date"].dt.date

    # Create dropdown filters
    customers = df["Customer"].dropna().unique()
    products = df["Customer Part No"].dropna().unique()

    selected_customer = st.sidebar.selectbox("Select Customer", customers)
    selected_product = st.sidebar.selectbox("Select Product", products)

    # Filtering
    df_filtered = df[
        (df["Customer"] == selected_customer) &
        (df["Customer Part No"] == selected_product)
    ]

    if df_filtered.empty:
        st.warning("No data available for selected customer/part.")
        st.stop()

    # Daily sum
    daily = df_filtered.groupby("Date")["EDI Quantity"].sum().reset_index()

    # Differences
    daily["Prev Day"] = daily["EDI Quantity"].shift(1)
    daily["Day Diff"] = daily["EDI Quantity"] - daily["Prev Day"]

    daily["Prev Week"] = daily["EDI Quantity"].shift(7)
    daily["Week Diff"] = daily["EDI Quantity"] - daily["Prev Week"]

    # Summary metrics
    col1, col2, col3 = st.columns(3)

    latest_qty = daily["EDI Quantity"].iloc[-1]
    day_diff = daily["Day Diff"].iloc[-1]
    week_diff = daily["Week Diff"].iloc[-1]

    col1.metric("Today Demand", f"{latest_qty}")
    col2.metric("Change vs Yesterday", f"{day_diff}")
    col3.metric("Change vs Last Week", f"{week_diff}")

    st.subheader("📉 Daily Trend")
    st.line_chart(daily.set_index("Date")["EDI Quantity"])

    st.subheader("📋 Data Table")
    st.dataframe(daily)
else:
    st.info("Upload a CSV file to view the dashboard.")
