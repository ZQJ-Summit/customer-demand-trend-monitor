import streamlit as st
import pandas as pd

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

    # Required columns based on your file
    required_cols = ["Ship Date", "Customer Code", "Customer Part No", "Order Quantity"]
    for col in required_cols:
        if col not in df.columns:
            st.error(f"Missing required column: {col}")
            st.stop()

    # Convert Ship Date to date
    df["Ship Date"] = pd.to_datetime(df["Ship Date"], errors="coerce")
    df["Date"] = df["Ship Date"].dt.date

    # Sidebar filters
    customers = df["Customer Code"].dropna().unique()
    products = df["Customer Part No"].dropna().unique()

    selected_customer = st.sidebar.selectbox("Select Customer", customers)
    selected_product = st.sidebar.selectbox("Select Product", products)

    # Filter data
    df_filtered = df[
        (df["Customer Code"] == selected_customer) &
        (df["Customer Part No"] == selected_product)
    ]

    if df_filtered.empty:
        st.warning("No data for selected customer & part number.")
        st.stop()

    # Daily aggregated data
    daily = df_filtered.groupby("Date")["Order Quantity"].sum().reset_index()

    # Calculate day-over-day & week-over-week
    daily["Prev Day"] = daily["Order Quantity"].shift(1)
    daily["Day Diff"] = daily["Order Quantity"] - daily["Prev Day"]

    daily["Prev Week"] = daily["Order Quantity"].shift(7)
    daily["Week Diff"] = daily["Order Quantity"] - daily["Prev Week"]

    # Summary metrics
    col1, col2, col3 = st.columns(3)

    latest_qty = daily["Order Quantity"].iloc[-1]
    day_diff = daily["Day Diff"].iloc[-1]
    week_diff = daily["Week Diff"].iloc[-1]

    col1.metric("Today Demand", f"{latest_qty}")
    col2.metric("Change vs Yesterday", f"{day_diff}")
    col3.metric("Change vs Last Week", f"{week_diff}")

    st.subheader("📉 Daily Trend")
    st.line_chart(daily.set_index("Date")["Order Quantity"])

    st.subheader("📋 Data Table")
    st.dataframe(daily)

else:
    st.info("Upload a CSV file to view the dashboard.")
