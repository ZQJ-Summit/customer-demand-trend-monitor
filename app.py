import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

# ---------------------------------------------
# Streamlit Page Config
# ---------------------------------------------
st.set_page_config(page_title="Customer Demand Trend Monitor", layout="wide")

st.title("📈 Customer Demand Trend Monitor")
st.write("Compare any two uploaded dates, with automatic historical storage in Neon.")


# ---------------------------------------------
# Function: Connect to Neon
# ---------------------------------------------
def get_conn():
    return psycopg2.connect(
        host=st.secrets["PGHOST"],
        database=st.secrets["PGDATABASE"],
        user=st.secrets["PGUSER"],
        password=st.secrets["PGPASSWORD"],
        port=st.secrets["PGPORT"]
    )


# ---------------------------------------------
# Step 1: Upload CSV → Insert into Neon DB
# ---------------------------------------------
uploaded_file = st.sidebar.file_uploader("Upload today's CSV", type=["csv"])

if uploaded_file:
    st.write("⏳ Processing your file...")

    try:
        df = pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Error reading CSV: {e}")
        st.stop()

    # Required Columns (based on your actual CSV)
    required = ["Ship Date", "Customer Code", "Customer Part No", "Order Quantity"]
    missing = [c for c in required if c not in df.columns]

    if missing:
        st.error(f"❌ Missing required columns: {missing}")
        st.stop()

    # Parse date
    df["Ship Date"] = pd.to_datetime(df["Ship Date"], errors="coerce").dt.date

    upload_date = datetime.now().date()
    total_rows = len(df)

    # Insert into Neon
    try:
        conn = get_conn()
        cur = conn.cursor()

        insert_sql = """
            INSERT INTO demand_history 
            (upload_date, ship_date, customer_code, customer_part_no, order_qty)
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

        st.success(f"✅ Uploaded {total_rows} rows into Neon (upload_date = {upload_date})")

    except Exception as e:
        st.error(f"❌ Database Insert Error: {e}")
        st.stop()


# ---------------------------------------------
# Step 2: Read last 150 days (5 months) from Neon
# ---------------------------------------------
try:
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
except Exception as e:
    st.error(f"❌ Database Query Error: {e}")
    st.stop()


# No Data Case
if not rows:
    st.info("No data yet. Upload your first CSV.")
    st.stop()


history = pd.DataFrame(rows, columns=[
    "upload_date", "ship_date", "customer_code", "customer_part_no", "order_qty"
])


# ---------------------------------------------
# Step 3: Sidebar Filters
# ---------------------------------------------
customers = sorted(history["customer_code"].unique())
selected_customer = st.sidebar.selectbox("Customer", customers)

products = sorted(history[history["customer_code"] == selected_customer]["customer_part_no"].unique())
selected_product = st.sidebar.selectbox("Product", products)

df_filtered = history[
    (history["customer_code"] == selected_customer) &
    (history["customer_part_no"] == selected_product)
]


# ---------------------------------------------
# Step 4: User Chooses Any Two Upload Dates
# ---------------------------------------------
upload_dates = sorted(df_filtered["upload_date"].unique())

if len(upload_dates) < 2:
    st.warning("Need at least TWO upload dates to compare trends.\nUpload 2 different files on different days.")
    st.stop()

date_a = st.sidebar.selectbox("Select Upload Date A", upload_dates, index=0)
date_b = st.sidebar.selectbox("Select Upload Date B", upload_dates, index=1)

df_a = df_filtered[df_filtered["upload_date"] == date_a]
df_b = df_filtered[df_filtered["upload_date"] == date_b]


# ---------------------------------------------
# Step 5: Aggregate Both Dates → Daily Trend
# ---------------------------------------------
daily_a = df_a.groupby("ship_date")["order_qty"].sum().reset_index()
daily_b = df_b.groupby("ship_date")["order_qty"].sum().reset_index()

daily_a.rename(columns={"order_qty": f"{date_a}"}, inplace=True)
daily_b.rename(columns={"order_qty": f"{date_b}"}, inplace=True)

merged = pd.merge(daily_a, daily_b, on="ship_date", how="outer").sort_values("ship_date").fillna(0)


# ---------------------------------------------
# Step 6: Plot & Table
# ---------------------------------------------
st.subheader(f"📉 Trend Comparison: {date_a} vs {date_b} (Last 5 Months)")
st.line_chart(merged.set_index("ship_date"))

st.subheader("📋 Data Table")
st.dataframe(merged)
