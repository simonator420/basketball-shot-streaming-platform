import streamlit as st
from utils.db import sql_to_pandas

st.title("🧍 Player overview")

runs = sql_to_pandas("""
    SELECT DISTINCT game_id, run_id
    FROM workspace_ingestion_data.analytics.player_stats
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

st.caption(f"Showing data for **Game {game_id}, Run {run_id}**")

top = sql_to_pandas(f"""
    SELECT
        player_id,
        total_points AS points
    FROM workspace_ingestion_data.analytics.player_stats
    WHERE game_id = {game_id}
      AND run_id = {run_id}
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
