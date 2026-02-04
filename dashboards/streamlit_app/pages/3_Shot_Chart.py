import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
from utils.db import sql_to_pandas
from utils.basketball_shot import BasketballShot
from utils.court_dimensions import get_court_lines

st.set_page_config(layout="wide")
st.title("🏀 3D Basketball Shot Chart")

# Get available game runs
runs = sql_to_pandas("""
    SELECT DISTINCT game_id, run_id
    FROM workspace_ingestion_data.analytics.shot_chart_data
    ORDER BY game_id DESC, run_id DESC
""")

runs["label"] = runs.apply(
    lambda r: f"Game {r.game_id} / Run {r.run_id}", axis=1
)

# Sidebar controls
selected = st.sidebar.selectbox(
    "Select game run",
    runs["label"].tolist()
)

selected_row = runs[runs["label"] == selected].iloc[0]
game_id = int(selected_row.game_id)
run_id = int(selected_row.run_id)

# Get players for selected game/run
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

# Sidebar options for visualization
show_made_only = st.sidebar.checkbox("Show made shots only", value=False)
show_missed_only = st.sidebar.checkbox("Show missed shots only", value=False)
shot_opacity = st.sidebar.slider("Shot path opacity", 0.1, 1.0, 0.55, 0.05)
peak_height = st.sidebar.slider("Shot arc height (feet)", 10, 20, 17, 1)

# Get shots data
shots = sql_to_pandas(f"""
    SELECT x, y, shot_outcome, shot_type, shot_distance, points_scored
    FROM workspace_ingestion_data.analytics.shot_chart_data
    WHERE game_id = {game_id}
      AND run_id = {run_id}
      AND player_id = {player_id}
    LIMIT 5000
""")

if shots.empty:
    st.warning(f"No shots found for Player {player_id}")
    st.stop()

# Add shot_made boolean column
shots['shot_made'] = shots['shot_outcome'].str.lower().isin(['made', 'make', 'scored'])

# Filter shots based on checkboxes
if show_made_only:
    shots = shots[shots['shot_made'] == True]
elif show_missed_only:
    shots = shots[shots['shot_made'] == False]

st.caption(f"Showing {len(shots)} shots for **Game {game_id}, Run {run_id}, Player {player_id}**")

# Create the court
court_lines_df = get_court_lines()

# Create the base figure with court lines
fig = px.line_3d(
    data_frame=court_lines_df, 
    x='x', 
    y='y', 
    z='z', 
    line_group='line_group', 
    color='color',
    color_discrete_map={
        'court': '#000000',
        'hoop': '#e47041'
    }
)

# Generate shot path coordinates for all shots
st.info(f"Generating 3D paths for {len(shots)} shots...")
all_shot_coords = pd.DataFrame()

for index, row in shots.iterrows():
    # Convert coordinates to match court dimensions
    # Assuming x, y are in standard shot chart coordinates
    shot_start_x = row['x']
    shot_start_y = row['y']
    
    # Create shot object
    shot = BasketballShot(
        shot_start_x=shot_start_x,
        shot_start_y=shot_start_y,
        shot_id=index,
        play_description=f"{row['shot_type']} - {row['shot_distance']:.1f}ft",
        shot_made=row['shot_made'],
        peak_height=peak_height
    )
    
    # Get shot path coordinates
    shot_df = shot.get_shot_path_coordinates()
    all_shot_coords = pd.concat([all_shot_coords, shot_df], ignore_index=True)

# Create shot paths visualization
if not all_shot_coords.empty:
    shot_path_fig = px.line_3d(
        data_frame=all_shot_coords,
        x='x',
        y='y',
        z='z',
        line_group='line_id',
        color='shot_result',
        color_discrete_map={
            'made': '#00ff00',
            'missed': '#ff0000'
        },
        custom_data=['description']
    )
    
    shot_path_fig.update_traces(opacity=shot_opacity)
    
    # Add shot paths to court figure
    for i in range(len(shot_path_fig.data)):
        fig.add_trace(shot_path_fig.data[i])

# Update layout for better viewing
fig.update_layout(
    scene=dict(
        xaxis=dict(title='Width (ft)', range=[0, 50]),
        yaxis=dict(title='Length (ft)', range=[0, 94]),
        zaxis=dict(title='Height (ft)', range=[0, 25]),
        aspectmode='manual',
        aspectratio=dict(x=50/94, y=1, z=25/94),  # Proper court proportions: width=50ft, length=94ft, height=25ft
        camera=dict(
            eye=dict(x=1.5, y=-1.5, z=1.2)
        )
    ),
    showlegend=True,
    legend=dict(
        yanchor="top",
        y=0.99,
        xanchor="left",
        x=0.01
    ),
    height=700,
    hovermode='closest'
)

# Display the 3D chart
st.plotly_chart(fig, use_container_width=True)

# Show statistics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Shots", len(shots))
with col2:
    made_shots = shots['shot_made'].sum()
    st.metric("Made Shots", made_shots)
with col3:
    if len(shots) > 0:
        fg_pct = (made_shots / len(shots)) * 100
        st.metric("FG%", f"{fg_pct:.1f}%")
    else:
        st.metric("FG%", "0%")
with col4:
    total_points = shots['points_scored'].sum()
    st.metric("Points Scored", int(total_points))

# Show sample data
with st.expander("📊 View shot data"):
    st.dataframe(shots.head(50), use_container_width=True)

# Add instructions
with st.expander("ℹ️ How to use"):
    st.markdown("""
    **Controls:**
    - Use your mouse to rotate, zoom, and pan the 3D view
    - Green paths = Made shots
    - Red paths = Missed shots
    - Adjust settings in the sidebar to customize the visualization
    
    **Tips:**
    - Higher arc height creates more pronounced shot paths
    - Lower opacity helps when viewing many overlapping shots
    - Use filters to focus on made or missed shots
    """)