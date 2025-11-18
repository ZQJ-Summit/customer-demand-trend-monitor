import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# -------------------------------------------------
# Streamlit Config
# -------------------------------------------------
st.set_page_config(page_title="Customer Demand Trend Monitor", layout="wide")

st.title("📈 Customer Demand Trend Monitor")
st.write("Compare any two uploaded dates with historical storage in Neon.")


# -------------------------------------------------
# Connect to Neon
# -------------------------------------------------
def get_conn():
    return psycopg2.connect(
        host=st.secrets["PGHOST"],
        database=st.secrets["PGDATABASE"],
        user=st.secrets["PGUSER"],
        password=st.secrets["PGPASSWORD"],
        port=st.secrets["PGPORT"],
    )


# -------------------------------------------------
# Normalize Column Names
# -------------------------------------------------
def normalize_cols(df):
    df.columns = (
        df.columns.str.strip()
        .str.replace("\n", " ")
        .str.replace("  ", " ")
        .str.lower()
    )
    return df


# -------------------------------------------------
# File Upload Section
# -------------------------------------------------
st.sidebar.header("Upload today's CSV")
uploaded_file = st.sidebar.file_uploader("Select CSV", type=["csv"])

df = None

if uploaded_file:
    st.info("⏳ Reading file...")

    try:
        df = pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"❌ Error reading CSV: {e}")
        st.stop()

    df = normalize_cols(df)

    required = {
        "ship date": "ship_date",
        "customer code": "customer_code",
        "customer part no": "customer_part_no",
        "order quantity": "order_qty",
    }

    missing = [c for c in required if c not in df.columns]
    if missing:
        st.error(f"❌ Missing required columns: {missing}")
        st.stop()

    df = df.rename(columns=required)
    df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce").dt.date

    st.success("File loaded successfully!")


# -------------------------------------------------
# Upload to Neon Button
# -------------------------------------------------
if df is not None and st.sidebar.button("📤 Upload to Neon"):

    try:
        conn = get_conn()
        cur = conn.cursor()

        insert_sql = """
            INSERT INTO demand_history
            (upload_date, ship_date, customer_code, customer_part_no, order_qty)
            VALUES %s
        """

        upload_date = datetime.now().date()

        values = [
            (
                upload_date,
                row["ship_date"],
                row["customer_code"],
                row["customer_part_no"],
                int(row["order_qty"]),
            )
            for _, row in df.iterrows()
        ]

        execute_values(cur, insert_sql, values)
        conn.commit()
        cur.close()
        conn.close()

        st.success(f"✅ Uploaded {len(df)} rows to Neon in seconds!")

    except Exception as e:
        st.error(f"❌ Neon insert error: {e}")


# -------------------------------------------------
# Load All History
# -------------------------------------------------
try:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT upload_date, ship_date, customer_code, customer_part_no, order_qty
        FROM demand_history
        ORDER BY ship_date;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

except Exception as e:
    st.error(f"❌ Neon read error: {e}")
    st.stop()

if not rows:
    st.info("Upload your first CSV to begin.")
    st.stop()

history = pd.DataFrame(rows, columns=[
    "upload_date", "ship_date", "customer_code", "customer_part_no", "order_qty"
])


# -------------------------------------------------
# Sidebar Filters
# -------------------------------------------------
st.sidebar.header("Filters")

customers = sorted(history["customer_code"].unique())
selected_customer = st.sidebar.selectbox("Customer", customers)

products = sorted(history[history["customer_code"] == selected_customer]["customer_part_no"].unique())
selected_product = st.sidebar.selectbox("Product", products)

df_filtered = history[
    (history["customer_code"] == selected_customer) &
    (history["customer_part_no"] == selected_product)
]


# -------------------------------------------------
# Upload Date Comparison
# -------------------------------------------------
upload_dates = sorted(df_filtered["upload_date"].unique())

if len(upload_dates) < 2:
    st.warning("Need at least two upload dates to compare.")
    st.stop()

date_a = st.sidebar.selectbox("Upload Date A", upload_dates)
date_b = st.sidebar.selectbox("Upload Date B", upload_dates)

df_a = df_filtered[df_filtered["upload_date"] == date_a]
df_b = df_filtered[df_filtered["upload_date"] == date_b]


# -------------------------------------------------
# Aggregate Trend Data
# -------------------------------------------------
daily_a = df_a.groupby("ship_date")["order_qty"].sum().reset_index()
daily_b = df_b.groupby("ship_date")["order_qty"].sum().reset_index()

daily_a.rename(columns={"order_qty": f"{date_a}"}, inplace=True)
daily_b.rename(columns={"order_qty": f"{date_b}"}, inplace=True)

merged = (
    pd.merge(daily_a, daily_b, on="ship_date", how="outer")
    .sort_values("ship_date")
    .fillna(0)
)

# -------------------------------------------------
# Plot Trend
# -------------------------------------------------
st.subheader(f"📉 Trend Comparison: {selected_customer} / {selected_product}")
st.line_chart(merged.set_index("ship_date"))

st.subheader("📋 Full Data")
st.dataframe(merged)
