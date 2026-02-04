import numpy as np
import pandas as pd


class BasketballShot:
    """
    Class to calculate and generate 3D coordinates for basketball shot paths.
    Based on parabolic trajectory with customizable peak height.
    """
    
    def __init__(self, shot_start_x, shot_start_y, shot_id, 
                 play_description="Shot", shot_made=False, 
                 peak_height=17, num_coords=100):
        """
        Initialize a basketball shot.
        
        Parameters:
        -----------
        shot_start_x : float
            Starting x-coordinate of the shot (court width)
        shot_start_y : float
            Starting y-coordinate of the shot (court length)
        shot_id : int
            Unique identifier for this shot
        play_description : str
            Description of the shot
        shot_made : bool
            Whether the shot was made or missed
        peak_height : float
            Peak height of the shot arc in feet (default: 17)
        num_coords : int
            Number of coordinates to generate for the path (default: 100)
        """
        self.shot_start_x = shot_start_x
        self.shot_start_y = shot_start_y
        self.shot_id = shot_id
        self.play_description = play_description
        self.shot_made = shot_made
        self.peak_height = peak_height
        self.num_coords = num_coords
        
        # Hoop parameters (centered on half court)
        self.hoop_x = 25  # Center of court width (50ft / 2)
        self.hoop_y = 4.25  # 4ft 3in from baseline
        self.hoop_z = 10  # 10ft high
        
        # Starting height (player release height)
        self.shot_start_z = 7  # Assume 7ft release height
        
    def _calculate_parabola_parameters(self):
        """
        Calculate the parabola parameters (a, h, k) for the shot path.
        Uses vertex form: z = a(x - h)^2 + k
        
        Returns:
        --------
        tuple: (a, h) where h is the x-coordinate of the vertex
        """
        x1 = self.shot_start_x
        z1 = self.shot_start_z
        x2 = self.hoop_x
        z2 = self.hoop_z
        k = self.peak_height  # Vertex z-coordinate (peak height)
        
        # Using the vertex form: z = a(x - h)^2 + k
        # We have two points (x1, z1) and (x2, z2)
        # Rearranging: a = (z - k) / (x - h)^2
        
        # Setting the two equations equal and solving for h using quadratic formula
        # a = (z1 - k) / (x1 - h)^2 = (z2 - k) / (x2 - h)^2
        # This leads to: (z1 - k)(x2 - h)^2 = (z2 - k)(x1 - h)^2
        
        # Expanding and rearranging into quadratic form: Ah^2 + Bh + C = 0
        A = (z1 - k) - (z2 - k)
        B = -2 * ((z1 - k) * x2 - (z2 - k) * x1)
        C = (z1 - k) * x2**2 - (z2 - k) * x1**2
        
        # Quadratic formula
        discriminant = B**2 - 4*A*C
        
        if abs(A) < 1e-10:  # Avoid division by zero
            # Linear case
            h = -C / B if abs(B) > 1e-10 else (x1 + x2) / 2
        else:
            # Take the solution that's between x1 and x2
            h1 = (-B + np.sqrt(abs(discriminant))) / (2*A)
            h2 = (-B - np.sqrt(abs(discriminant))) / (2*A)
            
            # Choose h that's between the start and hoop
            min_x = min(x1, x2)
            max_x = max(x1, x2)
            
            if min_x <= h1 <= max_x:
                h = h1
            elif min_x <= h2 <= max_x:
                h = h2
            else:
                h = (h1 + h2) / 2  # Average if neither is in range
        
        # Calculate a using one of the points
        a = (z1 - k) / ((x1 - h)**2) if abs(x1 - h) > 1e-10 else -0.1
        
        return a, h
    
    def get_shot_path_coordinates(self):
        """
        Generate all coordinates along the shot path.
        The trajectory starts from ground level (z=0) at the shot location,
        rises to the peak, and ends at the hoop.
        
        Returns:
        --------
        pd.DataFrame: DataFrame with columns [x, y, z, shot_coord_index, line_id, 
                                               description, shot_result]
        """
        shot_path_coords = []
        
        # Phase 1: Rise from ground (z=0) to release height
        # Add a point at ground level at the shot location
        shot_path_coords.append([
            self.shot_start_x,
            self.shot_start_y,
            0,  # Ground level
            0,
            f"shot_{self.shot_id}",
            self.play_description,
            "made" if self.shot_made else "missed"
        ])
        
        # Phase 2: Calculate parabolic trajectory from release point to hoop
        a, vertex_x = self._calculate_parabola_parameters()
        
        # Calculate y-axis shift (movement along court length)
        y_shift = self.hoop_y - self.shot_start_y
        y_shift_per_coord = y_shift / self.num_coords
        
        # Generate coordinates along the parabolic path
        x_coords = np.linspace(self.shot_start_x, self.hoop_x, self.num_coords + 1)
        
        for index, x in enumerate(x_coords):
            # Calculate z using parabola equation
            z = a * (x - vertex_x)**2 + self.peak_height
            
            # Calculate y (movement along court)
            y = self.shot_start_y + (y_shift_per_coord * index)
            
            shot_path_coords.append([
                x, 
                y, 
                z, 
                index + 1,  # Offset by 1 since we added ground point
                f"shot_{self.shot_id}",
                self.play_description,
                "made" if self.shot_made else "missed"
            ])
        
        # Create DataFrame
        df = pd.DataFrame(
            shot_path_coords, 
            columns=['x', 'y', 'z', 'shot_coord_index', 'line_id', 
                    'description', 'shot_result']
        )
        
        return df
    
    def get_shot_statistics(self):
        """
        Get statistics about the shot.
        
        Returns:
        --------
        dict: Dictionary with shot statistics
        """
        # Calculate shot distance (2D distance on court)
        distance = np.sqrt(
            (self.shot_start_x - self.hoop_x)**2 + 
            (self.shot_start_y - self.hoop_y)**2
        )
        
        return {
            'shot_id': self.shot_id,
            'distance': distance,
            'peak_height': self.peak_height,
            'made': self.shot_made,
            'description': self.play_description
        }