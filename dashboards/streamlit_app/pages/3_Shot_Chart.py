import streamlit as st
from utils.db import sql_to_pandas

st.title("🎯 Shot chart")

runs = sql_to_pandas("""
    SELECT DISTINCT game_id, run_id
    FROM workspace_ingestion_data.analytics.shot_chart_data
    ORDER BY game_id DESC, run_id DESC
""")

runs["label"] = runs.apply(
    lambda r: f"Game {r.game_id} / Run {r.run_id}", axis=1
)

selected = st.sidebar.selectbox(
    "Select game run",
    runs["label"].tolist()
)

selected_row = runs[runs["label"] == selected].iloc[0]
game_id = int(selected_row.game_id)
run_id = int(selected_row.run_id)

players = sql_to_pandas(f"""
    SELECT DISTINCT player_id
    FROM workspace_ingestion_data.analytics.shot_chart_data
    WHERE game_id = {game_id}
      AND run_id = {run_id}
    ORDER BY player_id
""")["player_id"].tolist()

if not players:
    st.warning("No players found for this game/run.")
    st.stop()

player_id = st.sidebar.selectbox("Player", players)

shots = sql_to_pandas(f"""
    SELECT x, y, shot_outcome, shot_type, shot_distance, points_scored
    FROM workspace_ingestion_data.analytics.shot_chart_data
    WHERE game_id = {game_id}
      AND run_id = {run_id}
      AND player_id = {player_id}
    LIMIT 5000
""")

st.caption(f"Showing shots for **Game {game_id}, Run {run_id}, Player {player_id}**")

st.subheader("Scatter (x,y)")
st.scatter_chart(shots, x="x", y="y")

st.subheader("Sample rows")
st.dataframe(shots.head(50), use_container_width=True)
