from shapely.geometry import Polygon, Point
import numpy as np


import pandas as pd
import geopandas as gpd
from shapely import wkb

def read_medellin(path_medellin):
    # Leer el archivo Parquet
    medellin_code = pd.read_parquet(path_medellin)

    # Convertir la columna 'geometry' de bytes a geometrías
    medellin_code['geometry'] = medellin_code['geometry'].apply(lambda x: wkb.loads(x, hex=True))

    # Convertir el DataFrame a un GeoDataFrame
    gdf_postal = gpd.GeoDataFrame(medellin_code, geometry='geometry')

    return gdf_postal



def generate_random_points(polygon, num_points):
    min_x, min_y, max_x, max_y = polygon.bounds
    points = []

    while len(points) < num_points:
        random_point = Point(np.random.uniform(min_x, max_x), np.random.uniform(min_y, max_y))
        if polygon.contains(random_point):
            points.append(random_point)
    
    return points

#random_points = generate_random_points(polygon, 1)