import streamlit as st
from utils.db import sql_to_pandas

st.title("📊 Game overview")

runs = sql_to_pandas("""
    SELECT DISTINCT run_id
    FROM workspace_ingestion_data.analytics.game_stats
    ORDER BY run_id DESC
""")["run_id"].tolist()

selected_run = st.sidebar.selectbox("Run", runs)


kpi = sql_to_pandas(f"""
    SELECT
      COUNT(DISTINCT game_id) AS games,
      SUM(total_points) AS points,
      SUM(total_shots) AS shots,
      AVG(overall_shooting_percentage)*100 AS fg_pct
    FROM workspace_ingestion_data.analytics.game_stats
    WHERE run_id = {int(selected_run)}
""").iloc[0]


c1, c2, c3, c4 = st.columns(4)
c1.metric("Games", int(kpi["games"] or 0))
c2.metric("Total points", int(kpi["points"] or 0))
c3.metric("Shots", int(kpi["shots"] or 0))
c4.metric("FG%", round(float(kpi["fg_pct"] or 0), 1))

df = sql_to_pandas(f"""
    SELECT
      game_id,
      total_points,
      total_shots,
      overall_shooting_percentage,
      avg_shot_distance,
      unique_players,
      last_updated
    FROM workspace_ingestion_data.analytics.game_stats
    WHERE run_id = {int(selected_run)}
    ORDER BY total_points DESC
    LIMIT 100
""")


st.subheader("Games")
st.dataframe(df, use_container_width=True)
