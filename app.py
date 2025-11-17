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
# Utility: Normalize column names
# ---------------------------------------------
def normalize_cols(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.replace("\n", " ")
        .str.replace("  ", " ")
        .str.lower()
    )
    return df


# ---------------------------------------------
# Step 1 — Upload CSV → Write into Neon
# ---------------------------------------------
uploaded_file = st.sidebar.file_uploader("Upload today's CSV", type=["csv"])

if uploaded_file:
    st.info("⏳ Processing your file...")

    try:
        df = pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"❌ CSV Read Error: {e}")
        st.stop()

    # Normalize column names
    df = normalize_cols(df)

    # Column name mapping
    col_map = {
        "ship date": "ship_date",
        "customer code": "customer_code",
        "customer part no": "customer_part_no",
        "order quantity": "order_qty",
    }

    # Validate required columns
    missing_cols = [c for c in col_map if c not in df.columns]
    if missing_cols:
        st.error(f"❌ Missing required columns: {missing_cols}")
        st.write("Your columns:", list(df.columns))
        st.stop()

    # Apply renaming
    df = df.rename(columns=col_map)

    # Parse date
    df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce").dt.date

    upload_date = datetime.now().date()
    total_rows = len(df)

    # Write into Neon
    try:
        conn = get_conn()
        cur = conn.cursor()

        insert_sql = """
            INSERT INTO demand_history
            (upload_date, ship_date, customer_code, customer_part_no, order_qty)
            VALUES (%s, %s, %s, %s, %s)
        """

        # Batch insert — VERY FAST
        rows_to_insert = [
            (
                upload_date,
                row["ship_date"],
                row["customer_code"],
                row["customer_part_no"],
                int(row["order_qty"]),
            )
            for _, row in df.iterrows()
        ]

        cur.executemany(insert_sql, rows_to_insert)
        conn.commit()
        cur.close()
        conn.close()

        st.success(f"✅ Uploaded {total_rows} rows into Neon (upload_date = {upload_date})")

    except Exception as e:
        st.error(f"❌ Database Insert Error: {e}")
        st.stop()


# ---------------------------------------------
# Step 2 — Read last 150 days from Neon
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

if not rows:
    st.info("No data yet. Upload your first CSV.")
    st.stop()


history = pd.DataFrame(rows, columns=[
    "upload_date", "ship_date", "customer_code", "customer_part_no", "order_qty"
])


# ---------------------------------------------
# Step 3 — Sidebar Filters
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
# Step 4 — User Chooses Two Upload Dates
# ---------------------------------------------
upload_dates = sorted(df_filtered["upload_date"].unique())

if len(upload_dates) < 2:
    st.warning("Need at least TWO upload dates to compare trends.")
    st.stop()

date_a = st.sidebar.selectbox("Select Upload Date A", upload_dates, index=0)
date_b = st.sidebar.selectbox("Select Upload Date B", upload_dates, index=1)

df_a = df_filtered[df_filtered["upload_date"] == date_a]
df_b = df_filtered[df_filtered["upload_date"] == date_b]


# ---------------------------------------------
# Step 5 — Aggregate & Merge
# ---------------------------------------------
daily_a = df_a.groupby("ship_date")["order_qty"].sum().reset_index()
daily_b = df_b.groupby("ship_date")["order_qty"].sum().reset_index()

daily_a.rename(columns={"order_qty": f"{date_a}"}, inplace=True)
daily_b.rename(columns={"order_qty": f"{date_b}"}, inplace=True)

merged = (
    pd.merge(daily_a, daily_b, on="ship_date", how="outer")
    .sort_values("ship_date")
    .fillna(0)
)


# ---------------------------------------------
# Step 6 — Plot + Table
# ---------------------------------------------
st.subheader(f"📉 Trend Comparison: {date_a} vs {date_b} (Last 5 Months)")
st.line_chart(merged.set_index("ship_date"))

st.subheader("📋 Data Table")
st.dataframe(merged)
