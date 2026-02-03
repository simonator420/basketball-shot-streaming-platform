import time
import streamlit as st
from utils.db import sql_to_pandas

st.set_page_config(page_title="Gold Dashboard", layout="wide")
st.title("🏀 Shot Lakehouse – Gold Dashboard")

auto_refresh = st.sidebar.toggle("Auto-refresh (30s)", value=True)

st.sidebar.markdown("---")
st.sidebar.caption("Data source: Gold Delta tables in Databricks")

df_latest = sql_to_pandas("""
SELECT
  MAX(last_updated) AS latest_ts
FROM workspace_ingestion_data.analytics.game_stats
""")

latest_ts = None
if not df_latest.empty and "latest_ts" in df_latest.columns:
    latest_ts = df_latest.loc[0, "latest_ts"]

if latest_ts is None:
    st.warning("No data / no permission to read analytics tables yet.")
else:
    st.success(f"Latest update in Gold: {latest_ts}")

st.write("Use the left menu to open a dashboard page (Game / Player / Shot Chart).")

if auto_refresh:
    time.sleep(30)
    st.rerun()