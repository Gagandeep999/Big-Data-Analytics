import os
import sys
import logging
from datetime import datetime
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import CountVectorizer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
import os
import numpy as np
from dask import persist
import dask.dataframe as df
from operator import add
from dask.distributed import Client
import matplotlib.pyplot as plt
from functools import reduce
import geopandas as gpd
from sklearn.metrics

from dask.distributed import Client

PROJ_DIR = os.getcwd().split(os.sep)[:-2]
sys.path.append(os.sep.join(PROJ_DIR))
from src.data import make_dataset
from src.visualization import visualize

DATA_DIR = os.path.join(os.sep.join(PROJ_DIR), 'data')
DATA_RAW_DIR = os.path.join(DATA_DIR, 'raw')
DATA_INTERIM_DIR = os.path.join(DATA_DIR, 'interim')
DATA_PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
MODEL_DIR = os.path.join(os.sep.join(PROJ_DIR), 'models')


log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)

NUM_OF_BOROUGH = 12

def load_model():
    return


def predict():
    pass


def assignCentroid(row, centroids):
    c = reduce((lambda a,b: a if a[1]<b[1] else b),[(centroids.name,\
        (row.nPositionCentreLongitude-centroids.nPositionCentreLongitude)**2 + (row.nPositionCentreLatitude-centroids.nPositionCentreLatitude)**2) \
            for centroids in centroids.itertuples()]) 
    return  c[0]


def k_means(park_spot_data):
    init_centroids = park_spot_data.sample(frac=0.002).head(NUM_OF_BOROUGH ,npartitions=-1)
    centroids = init_centroids.reset_index(drop=True).rename(columns={'sNoPlace':'name'})
    park_spot_data['centroids']=''
    epoch=0
    while True:
        epoch+=1
        park_spot_data['new_centroids'] = park_spot_data.apply(assignCentroid, axis=1, args=(centroids,), meta=('centroid', 'f8'))
        park_spot_data = park_spot_data.persist()
        spot_changed_number =len(park_spot_data[park_spot_data['centroids']!=park_spot_data['new_centroids']])
        logging.info('Epoch {0}: {1} spots re-assigned'.format(epoch,spot_changed_number))
        if spot_changed_number==0:
            return park_spot_data.compute()
        centroids = park_spot_data[['nPositionCentreLongitude','nPositionCentreLatitude','new_centroids']].groupby('new_centroids').mean().reset_index().rename(columns={'new_centroids':'name'})
        centroids = centroids.persist()
        park_spot_data['centroids']=park_spot_data['new_centroids']
    

if __name__ == '__main__':
    client = Client() # "tcp://127.0.0.1:58293"
    logging.info(client)
    park_spot_data = df.read_csv(os.path.join(DATA_INTERIM_DIR, 'spots_with_cities.csv'), 
        encoding='cp1252', blocksize='512KiB')[['sNoPlace','nPositionCentreLongitude','nPositionCentreLatitude']]
    clustered_data = k_means(park_spot_data)
    clustered_data_geo = gpd.GeoDataFrame(clustered_data, geometry=gpd.points_from_xy(clustered_data.nPositionCentreLongitude, clustered_data.nPositionCentreLatitude))
    boroughs = gpd.read_file(os.path.join(DATA_RAW_DIR, 'montreal_boroughs.geojson'))
    visualize.plot_parking_spots(clustered_data_geo, boroughs, 'Clustered parking spots after running k-means', 'Parking_spots_dask.png')
    y_true = clustered_data_geo['Cities']
    y_pred = clustered_data_geo['new_centroids']
    cm = confusion_matrix(y_true, y_pred, labels=spots_cities)
    logging.info(classification_report(y_true, y_pred, target_names=spots_cities, zero_division=0))