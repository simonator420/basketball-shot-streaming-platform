import streamlit as st
from utils.db import sql_to_pandas

st.title("🧍 Player overview")

dates = sql_to_pandas("""
    SELECT DISTINCT date
    FROM workspace_ingestion_data.analytics.player_stats
    ORDER BY date DESC
""")["date"].tolist()

selected_date = st.sidebar.selectbox("Date", dates)

top = sql_to_pandas(f"""
    SELECT player_id, SUM(total_points) AS points
    FROM workspace_ingestion_data.analytics.player_stats
    WHERE date = DATE('{selected_date}')
    GROUP BY player_id
    ORDER BY points DESC
    LIMIT 15
""")

st.subheader("Top scorers")
st.bar_chart(top.set_index("player_id")["points"])
st.dataframe(top, use_container_width=True)

st.subheader("Player summary (career)")
summary = sql_to_pandas("""
    SELECT *
    FROM workspace_ingestion_data.analytics.player_performance_summary
    ORDER BY career_points DESC
    LIMIT 50
""")
st.dataframe(summary, use_container_width=True)
