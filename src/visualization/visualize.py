import os
import sys
import logging
import pandas as pd
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.neighbors import DistanceMetric
import math
from math import sin, cos, sqrt, atan2, radians

log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)

PROJ_DIR = os.getcwd().split(os.sep)[:-2]
DATA_DIR = os.path.join(os.sep.join(PROJ_DIR), 'data')
DATA_RAW_DIR = os.path.join(DATA_DIR, 'raw')
RPT_DIR = os.path.join(os.sep.join(PROJ_DIR), 'reports')
RPT_FIGURES_DIR = os.path.join(RPT_DIR, 'figures')

# a method that takes as input year and saves a png image in the reference folder
def towing_per_year_visual(year, towings, boroughs):
    logging.info('Visualizations for the year {0}...'.format(year))
    output_filename = year+'_towings.png'
    fig, ax = plt.subplots(1, figsize=(8, 4))
    boroughs.plot(ax=ax)
    towings.plot(ax=ax, marker='o', color='r', alpha=0.075)
    plt.title('Towing in the year {0}'.format(year))
    plt.savefig(os.path.join(RPT_FIGURES_DIR, output_filename))


def parking_spots(spots, boroughs):
    logging.info('Visualizations for parking spots...')
    output_filename = 'Parking_spots.png'
    fig, ax = plt.subplots(1, figsize=(8, 4))
    boroughs.plot(ax=ax)
    spots.plot(ax=ax, marker='o', cmap = "hsv", alpha=0.075)
    plt.title('Parking Spots of Montreal')
    plt.savefig(os.path.join(RPT_FIGURES_DIR, output_filename))


def generate_visuals(df_remor, df_spots, boroughs):
    logging.info('Generating visualizations per year...')
    for year in range(2015, 2021, 1):
        df_remor_per_year = df_remor[df_remor['DATE_ORIGINE'].dt.strftime('%Y') == str(year)]
        towings = gpd.GeoDataFrame(df_remor_per_year, geometry=gpd.points_from_xy(df_remor_per_year.LONGITUDE_ORIGINE, df_remor_per_year.LATITUDE_ORIGINE))
        towing_per_year_visual(str(year), towings, boroughs)
    logging.info('Generating visualizations for parking spots...')
    spots = gpd.GeoDataFrame(df_spots, geometry=gpd.points_from_xy(df_spots.nPositionCentreLongitude, df_spots.nPositionCentreLatitude))
    parking_spots(spots, boroughs)


if __name__=='__main__':
    logging.info('Running scripts to generate visualizations...')
    df_remorquages = pd.read_csv(os.path.join(DATA_RAW_DIR, 'remorquages.csv'), header=0)
    df_spots = pd.read_csv(os.path.join(DATA_RAW_DIR, 'Places.csv'), header=0, encoding='cp1252')
    df_remorquages['DATE_ORIGINE'] = pd.to_datetime(df_remorquages['DATE_ORIGINE'])
    boroughs = gpd.read_file(os.path.join(DATA_RAW_DIR, 'montreal_boroughs.geojson'))
    generate_visuals(df_remorquages, df_spots, boroughs)
