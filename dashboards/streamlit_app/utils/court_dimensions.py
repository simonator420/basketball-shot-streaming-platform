import pandas as pd
import numpy as np


def get_court_lines():
    """
    Generate all court line coordinates for a basketball half-court.
    Based on NCAA/NBA regulation dimensions.
    
    Returns:
    --------
    pd.DataFrame: DataFrame with columns [x, y, z, line_group, color]
    """
    
    # Court dimensions (in feet)
    width = 50  # Full court width
    length = 94  # Full court length
    
    all_lines = []
    
    # ===== COURT PERIMETER =====
    court_perimeter = [
        [0, 0, 0],
        [width, 0, 0],
        [width, length, 0],
        [0, length, 0],
        [0, 0, 0]
    ]
    for point in court_perimeter:
        all_lines.append(point + ['outside_perimeter', 'court'])
    
    # ===== HALF COURT LINE =====
    half_court = [
        [0, length/2, 0],
        [width, length/2, 0]
    ]
    for point in half_court:
        all_lines.append(point + ['half_court', 'court'])
    
    # ===== PAINT/KEY AREA (16ft wide, 19ft long from baseline) =====
    paint_width = 16
    paint_length = 19
    paint_x_start = (width - paint_width) / 2
    
    # Near baseline paint (closed rectangle)
    near_paint = [
        [paint_x_start, 0, 0],
        [paint_x_start, paint_length, 0],
        [paint_x_start + paint_width, paint_length, 0],
        [paint_x_start + paint_width, 0, 0],
        [paint_x_start, 0, 0]  # Close the rectangle
    ]
    for point in near_paint:
        all_lines.append(point + ['near_paint', 'court'])
    
    # Far baseline paint (closed rectangle)
    far_paint = [
        [paint_x_start, length, 0],
        [paint_x_start, length - paint_length, 0],
        [paint_x_start + paint_width, length - paint_length, 0],
        [paint_x_start + paint_width, length, 0],
        [paint_x_start, length, 0]  # Close the rectangle
    ]
    for point in far_paint:
        all_lines.append(point + ['far_paint', 'court'])
    
    # ===== BACKBOARDS (6ft wide, 4ft tall, 3ft from baseline, 9ft from ground) =====
    backboard_width = 6
    backboard_height = 4
    backboard_offset = 3
    backboard_ground_offset = 9
    backboard_x_start = (width - backboard_width) / 2
    
    # Near backboard
    near_backboard = [
        [backboard_x_start, backboard_offset, backboard_ground_offset],
        [backboard_x_start + backboard_width, backboard_offset, backboard_ground_offset],
        [backboard_x_start + backboard_width, backboard_offset, backboard_ground_offset + backboard_height],
        [backboard_x_start, backboard_offset, backboard_ground_offset + backboard_height],
        [backboard_x_start, backboard_offset, backboard_ground_offset]
    ]
    for point in near_backboard:
        all_lines.append(point + ['near_backboard', 'court'])
    
    # Far backboard
    far_backboard = [
        [backboard_x_start, length - backboard_offset, backboard_ground_offset],
        [backboard_x_start + backboard_width, length - backboard_offset, backboard_ground_offset],
        [backboard_x_start + backboard_width, length - backboard_offset, backboard_ground_offset + backboard_height],
        [backboard_x_start, length - backboard_offset, backboard_ground_offset + backboard_height],
        [backboard_x_start, length - backboard_offset, backboard_ground_offset]
    ]
    for point in far_backboard:
        all_lines.append(point + ['far_backboard', 'court'])
    
    # ===== HOOPS (radius 9 inches = 0.75 ft, center 4.25ft from baseline, 10ft high) =====
    hoop_radius = 0.75
    hoop_offset = 4.25
    hoop_height = 10
    hoop_center_x = width / 2
    
    # Generate circle coordinates
    num_points = 50
    angles = np.linspace(0, 2*np.pi, num_points)
    
    # Near hoop
    for angle in angles:
        x = hoop_center_x + hoop_radius * np.cos(angle)
        y = hoop_offset
        z = hoop_height
        all_lines.append([x, y, z, 'near_hoop', 'hoop'])
    
    # Far hoop
    for angle in angles:
        x = hoop_center_x + hoop_radius * np.cos(angle)
        y = length - hoop_offset
        z = hoop_height
        all_lines.append([x, y, z, 'far_hoop', 'hoop'])
    
    # ===== THREE-POINT LINE =====
    # Arc radius: 22.15 ft (22ft 1.75in)
    # Straight portion: 21.65 ft from center (21ft 8in)
    three_point_radius = 22.15
    three_point_straight_dist = 21.65
    straight_portion_length = 8.9  # 8ft 10.75in
    
    # Near three-point line
    # Left straight portion
    y_straight_end = hoop_offset + np.sqrt(three_point_radius**2 - three_point_straight_dist**2)
    all_lines.append([hoop_center_x - three_point_straight_dist, 0, 0, 'near_three_left', 'court'])
    all_lines.append([hoop_center_x - three_point_straight_dist, y_straight_end, 0, 'near_three_left', 'court'])
    
    # Arc portion
    start_angle = np.arctan2(y_straight_end - hoop_offset, -three_point_straight_dist)
    end_angle = np.pi - start_angle
    arc_angles = np.linspace(start_angle, end_angle, 50)
    
    for angle in arc_angles:
        x = hoop_center_x + three_point_radius * np.cos(angle)
        y = hoop_offset + three_point_radius * np.sin(angle)
        all_lines.append([x, y, 0, 'near_three_arc', 'court'])
    
    # Right straight portion
    all_lines.append([hoop_center_x + three_point_straight_dist, y_straight_end, 0, 'near_three_right', 'court'])
    all_lines.append([hoop_center_x + three_point_straight_dist, 0, 0, 'near_three_right', 'court'])
    
    # Far three-point line (mirror)
    # Left straight portion
    all_lines.append([hoop_center_x - three_point_straight_dist, length, 0, 'far_three_left', 'court'])
    all_lines.append([hoop_center_x - three_point_straight_dist, length - y_straight_end, 0, 'far_three_left', 'court'])
    
    # Arc portion
    for angle in arc_angles:
        x = hoop_center_x + three_point_radius * np.cos(angle)
        y = (length - hoop_offset) - three_point_radius * np.sin(angle)
        all_lines.append([x, y, 0, 'far_three_arc', 'court'])
    
    # Right straight portion
    all_lines.append([hoop_center_x + three_point_straight_dist, length - y_straight_end, 0, 'far_three_right', 'court'])
    all_lines.append([hoop_center_x + three_point_straight_dist, length, 0, 'far_three_right', 'court'])
    
    # ===== FREE THROW CIRCLES (6ft radius) =====
    ft_radius = 6
    ft_distance_from_baseline = 19  # Center of circle is 19ft from baseline
    
    # Near free throw circle
    for angle in np.linspace(0, np.pi, 30):  # Half circle
        x = hoop_center_x + ft_radius * np.cos(angle)
        y = ft_distance_from_baseline + ft_radius * np.sin(angle)
        all_lines.append([x, y, 0, 'near_ft_circle', 'court'])
    
    # Far free throw circle
    for angle in np.linspace(0, np.pi, 30):
        x = hoop_center_x + ft_radius * np.cos(angle)
        y = (length - ft_distance_from_baseline) - ft_radius * np.sin(angle)
        all_lines.append([x, y, 0, 'far_ft_circle', 'court'])
    
    # Create DataFrame
    court_df = pd.DataFrame(
        all_lines,
        columns=['x', 'y', 'z', 'line_group', 'color']
    )
    
    return court_df


def get_half_court_lines():
    """
    Generate court lines for just a half-court view.
    
    Returns:
    --------
    pd.DataFrame: DataFrame with columns [x, y, z, line_group, color]
    """
    full_court = get_court_lines()
    
    # Filter to only show one half of the court (y < 47)
    half_court = full_court[
        (full_court['line_group'].str.contains('near_')) | 
        (full_court['line_group'].str.contains('outside_perimeter')) |
        (full_court['y'] <= 47)
    ].copy()
    
    return half_court