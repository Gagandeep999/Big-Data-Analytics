import os
import sys
import logging
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJ_DIR = os.getcwd().split(os.sep)[:-2]
sys.path.append(os.sep.join(PROJ_DIR))
from src.data import make_dataset

DATA_DIR = os.path.join(os.sep.join(PROJ_DIR), 'data')
DATA_RAW_DIR = os.path.join(DATA_DIR, 'raw')
DATA_INTERIM_DIR = os.path.join(DATA_DIR, 'interim')
DATA_PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')


log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)


def distance_bw_parking_spots(data):
    if not isinstance(data, DataFrame):
        raise ValueError('Type pass to distance_bw_parking_spots() should be DataFrame.')
    data = data.\
        withColumn('LONGITUDE_ORIGINE_rad', F.expr('radians(LONGITUDE_ORIGINE)')).\
        withColumn('LATITUDE_ORIGINE_rad', F.expr('radians(LATITUDE_ORIGINE)')).\
        withColumn('LONGITUDE_DESTINATION_rad', F.expr('radians(LONGITUDE_DESTINATION)')).\
        withColumn('LATITUDE_DESTINATION_rad', F.expr('radians(LATITUDE_DESTINATION)')).\
        withColumn('Diff_long', F.expr('(LONGITUDE_DESTINATION_rad-LONGITUDE_ORIGINE_rad)/2')).\
        withColumn('Diff_lat', F.expr('(LONGITUDE_DESTINATION_rad-LONGITUDE_ORIGINE_rad)/2')).\
        withColumn('LATITUDE_DESTINATION_cos', F.expr('cos(LATITUDE_DESTINATION_rad)')).\
        withColumn('LATITUDE_ORIGINE_cos', F.expr('cos(LATITUDE_ORIGINE_rad)')).\
        withColumn('Diff_long', F.expr('sin(Diff_long)')).\
        withColumn('Diff_lat', F.expr('sin(Diff_lat)')).\
        withColumn('A', F.expr('Diff_lat*Diff_lat + LATITUDE_DESTINATION_cos * LATITUDE_ORIGINE_cos * Diff_long * Diff_long')).\
        withColumn('One_minus_A', F.expr('1-A')).\
        withColumn('C', F.expr('2 * atan2( sqrt(A), sqrt(One_minus_A))')).\
        withColumn('Distance_km', F.expr('6373.0*C'))

    # cols_needed = ['DATE_ORIGINE', 'LONGITUDE_ORIGINE', 'LATITUDE_ORIGINE', 'Distance(Km)', 'MOTIF_REMORQUAGE']
    
    df_final = data.select('DATE_ORIGINE', 'LONGITUDE_ORIGINE', 'LATITUDE_ORIGINE', 'Distance_km', 'MOTIF_REMORQUAGE')
    try:
        assert df_final.count()==250077
    except AssertionError:
        logging.error('Final count does not match before removing NA. Saving to file anyways...')

    df_final = df_final.na.drop()

    try:
        assert df_final.count()==248476
    except AssertionError:
        logging.error('Final count does not match after removing NA. Saving to file anyways...')
    
    return df_final


def get_necessary_columns_from_weather_data(data):
    if not isinstance(data, DataFrame):
        raise ValueError('Incorrect parameter type for get_necessary_columns_from_weather_data() method')
    df_final = data.select('Date_Time', 'Year', 'Month', 'Day', 'Mean_Temp', 'Total_Rain', 'Total_Precip', 'Total_Snow', 'Spd_of_Max_Gust')
    return df_final


def combine_towing_and_weaher_data():
    pass


if __name__ == '__main__':
    towing_file = os.path.join(DATA_INTERIM_DIR, 'towing.data')
    weather_file = os.path.join(DATA_INTERIM_DIR, 'weather.data')
    spark = make_dataset.init_spark()
    towing_data = make_dataset.load_data_parquet(spark, towing_file)
    weather_data = make_dataset.load_data_parquet(spark, weather_file)
    towing_distance = distance_bw_parking_spots(towing_data)
    weather_features_needed = get_necessary_columns_from_weather_data(weather_data)

    towing_file = os.path.join(DATA_INTERIM_DIR, 'towing_needed.data')
    weather_file = os.path.join(DATA_INTERIM_DIR, 'weather_needed.data')
    make_dataset.save_data(towing_distance, towing_file)
    make_dataset.save_data(weather_features_needed, weather_file)

    