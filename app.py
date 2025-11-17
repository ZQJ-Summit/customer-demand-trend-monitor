import streamlit as st
import pandas as pd

st.set_page_config(page_title="Customer Demand Trend Monitor", layout="wide")
st.title("📈 Customer Demand Trend Monitor")
st.write("Compare daily demand trend between two uploaded files.")

# -----------------------------------------
# Upload yesterday & today files
# -----------------------------------------
st.sidebar.header("Upload Files")
yesterday_file = st.sidebar.file_uploader("Upload Yesterday's CSV", type=["csv"])
today_file = st.sidebar.file_uploader("Upload Today's CSV", type=["csv"])

if yesterday_file and today_file:
    df_old = pd.read_csv(yesterday_file)
    df_new = pd.read_csv(today_file)

    required_cols = ["Ship Date", "Customer Code", "Customer Part No", "Order Quantity"]

    for col in required_cols:
        if col not in df_old.columns or col not in df_new.columns:
            st.error(f"Missing required column: {col}")
            st.stop()

    # Parse date
    df_old["Ship Date"] = pd.to_datetime(df_old["Ship Date"], errors="coerce")
    df_new["Ship Date"] = pd.to_datetime(df_new["Ship Date"], errors="coerce")

    df_old["Date"] = df_old["Ship Date"].dt.date
    df_new["Date"] = df_new["Ship Date"].dt.date

    # Filters
    customers = sorted(set(df_old["Customer Code"]).union(df_new["Customer Code"]))
    products = sorted(set(df_old["Customer Part No"]).union(df_new["Customer Part No"]))

    selected_customer = st.sidebar.selectbox("Select Customer", customers)
    selected_product = st.sidebar.selectbox("Select Product", products)

    # Filter both datasets
    old_filtered = df_old[
        (df_old["Customer Code"] == selected_customer) &
        (df_old["Customer Part No"] == selected_product)
    ]

    new_filtered = df_new[
        (df_new["Customer Code"] == selected_customer) &
        (df_new["Customer Part No"] == selected_product)
    ]

    if old_filtered.empty or new_filtered.empty:
        st.warning("Selected customer/product not found in both files.")
        st.stop()

    # Aggregate
    daily_old = old_filtered.groupby("Date")["Order Quantity"].sum().reset_index()
    daily_new = new_filtered.groupby("Date")["Order Quantity"].sum().reset_index()

    daily_old = daily_old.rename(columns={"Order Quantity": "Yesterday"})
    daily_new = daily_new.rename(columns={"Order Quantity": "Today"})

    # Merge for comparison
    merged = pd.merge(daily_old, daily_new, on="Date", how="outer").sort_values("Date")
    
    if not merged.empty:
        latest_date = merged["Date"].max()
        five_months_ago = latest_date - pd.Timedelta(days=150)
        merged = merged[merged["Date"] >= five_months_ago]
    

    st.subheader("📉 Daily Demand Comparison")
    st.line_chart(merged.set_index("Date"))

    st.subheader("📋 Data Table")
    st.dataframe(merged)

else:
    st.info("Please upload both Yesterday and Today CSV files.")
