import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

st.set_page_config(page_title="Customer Demand Trend Monitor", layout="wide")

st.title("📈 Customer Demand Trend Monitor")
st.write("Compare any two uploaded dates, with automatic historical storage in Neon.")

# ----------------------------
# Connect to Neon
# ----------------------------
def get_conn():
    return psycopg2.connect(
        host=st.secrets["PGHOST"],
        database=st.secrets["PGDATABASE"],
        user=st.secrets["PGUSER"],
        password=st.secrets["PGPASSWORD"],
        port=st.secrets["PGPORT"]
    )

# ----------------------------
# Step 1: Upload CSV → Write to Neon
# ----------------------------
uploaded_file = st.sidebar.file_uploader("Upload today's CSV", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)

    required_cols = ["Ship Date", "Customer Code", "Customer Part No", "Order Quantity"]
    for col in required_cols:
        if col not in df.columns:
            st.error(f"Missing required column: {col}")
            st.stop()

    df["Ship Date"] = pd.to_datetime(df["Ship Date"], errors="coerce").dt.date
    upload_date = datetime.now().date()

    conn = get_conn()
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO demand_history (upload_date, ship_date, customer_code, customer_part_no, order_qty)
        VALUES (%s, %s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        cur.execute(insert_sql, (
            upload_date,
            row["Ship Date"],
            row["Customer Code"],
            row["Customer Part No"],
            int(row["Order Quantity"])
        ))

    conn.commit()
    cur.close()
    conn.close()

    st.success(f"Uploaded {len(df)} rows into Neon for date {upload_date}")

# ----------------------------
# Step 2: Read recent 5 months from Neon
# ----------------------------
conn = get_conn()
cur = conn.cursor()

cur.execute("""
    SELECT upload_date, ship_date, customer_code, customer_part_no, order_qty
    FROM demand_history
    WHERE ship_date >= CURRENT_DATE - INTERVAL '150 days'
    ORDER BY ship_date;
""")

rows = cur.fetchall()
cur.close()
conn.close()

if not rows:
    st.info("No data yet. Upload your first CSV.")
    st.stop()

history = pd.DataFrame(rows, columns=[
    "upload_date", "ship_date", "customer_code", "customer_part_no", "order_qty"
])

# ----------------------------
# Step 3: Filters
# ----------------------------
customers = history["customer_code"].unique()
selected_customer = st.sidebar.selectbox("Customer", customers)

products = history[history["customer_code"] == selected_customer]["customer_part_no"].unique()
selected_product = st.sidebar.selectbox("Product", products)

df_filtered = history[
    (history["customer_code"] == selected_customer) &
    (history["customer_part_no"] == selected_product)
]

# ----------------------------
# Step 4: Select ANY two upload dates to compare
# ----------------------------
available_dates = sorted(df_filtered["upload_date"].unique())

if len(available_dates) < 2:
    st.warning("Need at least two upload dates to compare.")
    st.stop()

date1 = st.sidebar.selectbox("Select Upload Date A", available_dates, index=0)
date2 = st.sidebar.selectbox("Select Upload Date B", available_dates, index=1)

df_a = df_filtered[df_filtered["upload_date"] == date1]
df_b = df_filtered[df_filtered["upload_date"] == date2]

# aggregate
daily_a = df_a.groupby("ship_date")["order_qty"].sum().reset_index()
daily_b = df_b.groupby("ship_date")["order_qty"].sum().reset_index()

daily_a.rename(columns={"order_qty": f"{date1}"}, inplace=True)
daily_b.rename(columns={"order_qty": f"{date2}"}, inplace=True)

# merge
merged = pd.merge(daily_a, daily_b, on="ship_date", how="outer").sort_values("ship_date")
merged = merged.fillna(0)

# ----------------------------
# Step 5: Plot
# ----------------------------
st.subheader(f"📉 Comparison Between {date1} and {date2} (Last 5 Months)")
st.line_chart(merged.set_index("ship_date"))

st.subheader("📋 Data Table")
st.dataframe(merged)
