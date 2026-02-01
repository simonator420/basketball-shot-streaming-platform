import streamlit as st
from utils.db import sql_to_pandas

st.title("🎯 Shot chart")

dates = sql_to_pandas("""
    SELECT DISTINCT date
    FROM workspace_ingestion_data.analytics.shot_chart_data
    ORDER BY date DESC
""")["date"].tolist()

selected_date = st.sidebar.selectbox("Date", dates)

players = sql_to_pandas(f"""
    SELECT DISTINCT player_id
    FROM workspace_ingestion_data.analytics.shot_chart_data
    WHERE date = DATE('{selected_date}')
    ORDER BY player_id
""")["player_id"].tolist()

player_id = st.sidebar.selectbox("Player", players)

shots = sql_to_pandas(f"""
    SELECT x, y, shot_outcome, shot_type, shot_distance, points_scored
    FROM workspace_ingestion_data.analytics.shot_chart_data
    WHERE date = DATE('{selected_date}')
      AND player_id = {player_id}
    LIMIT 5000
""")

st.subheader("Scatter (x,y)")
st.scatter_chart(shots, x="x", y="y")

st.subheader("Sample rows")
st.dataframe(shots.head(50), use_container_width=True)
