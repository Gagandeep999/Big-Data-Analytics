import os
import json
import logging
import pandas as pd
import geopandas as gpd
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from shapely.ops import nearest_points
from shapely.geometry import Point, mapping, shape


log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)

PROJ_DIR = os.getcwd().split(os.sep)[:-2]
DATA_DIR = os.path.join(os.sep.join(PROJ_DIR), 'data')
DATA_RAW_DIR = os.path.join(DATA_DIR, 'raw')
DATA_INTERIM_DIR = os.path.join(DATA_DIR, 'interim')


with open(os.path.join(DATA_RAW_DIR, 'montreal_boroughs.geojson')) as f:
    geojson = json.load(f)


def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


def toCSVLineRDD(rdd):
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x,y: os.linesep.join([x,y]))
    return a + os.linesep


def toCSVLine(data):
    if isinstance(data, RDD):
        if data.count() > 0:
            return toCSVLineRDD(data)
        else:
            return ""
    elif isinstance(data, DataFrame):
        if data.count() > 0:
            return toCSVLineRDD(data.rdd)
        else:
            return ""
    return None


def get_city(ref_point):
    min_distance = None
    features = []
    for i, feature in enumerate(geojson["features"]):
        p1, p2 = nearest_points(shape(feature["geometry"]), ref_point)
        distance = p1.distance(p2)
        prop_org = feature["properties"]
        if min_distance is None or distance < min_distance:
            min_distance = distance
        feature["prop"] = {"distance": distance, "properties":prop_org}
        features.append(feature)
        features.append(
            {
                "type": "Feature",
                "prop": {"distance": distance, "properties":prop_org},
                "geometry": mapping(p1),
            }
        )

    features = sorted(features, key=lambda x: x["prop"]["distance"])
    features.append(
        {
            "type": "Feature",
            "prop": {"distance": 0, "properties":prop_org},
            "geometry": mapping(ref_point),
        }
    )
    return features[0]['properties']['NOM']


def load_data(spark, filename):
    logging.info('Loading data...')
    data = spark.read.csv(filename, header=True)
    return data


def make_towing_data(spark, filename):
    logging.info('Making towing data ...')
    data = load_data(spark, filename)
    return data


def make_weather_data(spark, filename):
    logging.info('Making weather data ...')
    data = load_data(spark, filename)
    return data


def make_spots_data(spots):
    logging.info('Making parking_spots data ...')
    df_spots = pd.read_csv(spots, header=0, encoding='cp1252')
    df_spots = gpd.GeoDataFrame(df_spots, geometry=gpd.points_from_xy(df_spots.nPositionCentreLongitude, df_spots.nPositionCentreLatitude))
    df_spots['Cities'] = df_spots['geometry'].map(get_city)
    return df_spots


def format_towing_data(data):
    logging.info('Formatting towing data ...')
    data_towing = data.withColumn('DATE_ORIGINE', F.concat(data.DATE_ORIGINE.substr(0,10)))
    return data_towing


def format_weather_data(data):
    logging.info('Formatting weather data ...')
    data_weather = data.withColumnRenamed('Longitude (x)', 'Longitude').\
                        withColumnRenamed('Latitude (y)', 'Latitude').\
                        withColumnRenamed('Station Name', 'Station_Name').\
                        withColumnRenamed('Climate ID', 'Climate_ID').\
                        withColumnRenamed('Date/Time', 'Date_Time').\
                        withColumnRenamed('Data Quality', 'Data_Quality').\
                        withColumnRenamed('Max Temp (°C)', 'Max_Temp').\
                        withColumnRenamed('Max Temp Flag', 'Max_Temp_Flag').\
                        withColumnRenamed('Min Temp (°C)', 'Min_Temp').\
                        withColumnRenamed('Min Temp Flag', 'Min_Temp_Flag').\
                        withColumnRenamed('Mean Temp (°C)', 'Mean_Temp').\
                        withColumnRenamed('Mean Temp Flag', 'Mean_Temp_Flag').\
                        withColumnRenamed('Heat Deg Days (°C)', 'Heat_Deg_Days').\
                        withColumnRenamed('Heat Deg Days Flag', 'Heat_Deg_Days_Flag').\
                        withColumnRenamed('Cool Deg Days (°C)', 'Cool_Deg_Days').\
                        withColumnRenamed('Cool Deg Days Flag', 'Cool_Deg_Days_Flag').\
                        withColumnRenamed('Total Rain (mm)', 'Total_Rain').\
                        withColumnRenamed('Total Rain Flag', 'Total_Rain_Flag').\
                        withColumnRenamed('Total Snow (cm)', 'Total_Snow').\
                        withColumnRenamed('Total Snow Flag', 'Total_Snow_Flag').\
                        withColumnRenamed('Total Precip (mm)', 'Total_Precip').\
                        withColumnRenamed('Total Precip Flag', 'Total_Precip_Flag').\
                        withColumnRenamed('Snow on Grnd (cm)', 'Snow_on_Grnd').\
                        withColumnRenamed('Snow on Grnd Flag', 'Snow_on_Grnd_Flag').\
                        withColumnRenamed('Dir of Max Gust (10s deg)', 'Dir_of_Max_Gust').\
                        withColumnRenamed('Dir of Max Gust Flag', 'Dir_of_Max_Gust_Flag').\
                        withColumnRenamed('Spd of Max Gust (km/h)', 'Spd_of_Max_Gust').\
                        withColumnRenamed('Spd of Max Gust Flag', 'Spd_of_Max_Gust_Flag')
    return data_weather


def save_data(data, filename):
    if not isinstance(data, DataFrame):
        raise ValueError('Cannot save towing data. It is not a PySpark DataFrame object')
    logging.info('Saving data as parquet file ...')
    data.write.mode('overwrite').parquet(os.path.join(DATA_INTERIM_DIR, filename))


def save_spots_data(data, filename):
    data.to_csv(path_or_buf=filename, columns=['sNoPlace', 'nPositionCentreLongitude', 'nPositionCentreLatitude', 'geometry', 'Cities'], header=True)


def load_data_parquet(spark, filename):
    data = spark.read.parquet(filename)
    return data


if __name__ == '__main__':
    logging.info('Initiating Spark...')
    spark = init_spark()

    towing_file = os.path.join(DATA_RAW_DIR, 'remorquages.csv')
    towing_data = make_towing_data(spark, towing_file)
    towing_data = format_towing_data(towing_data)
    save_data(towing_data, 'towing.data')

    for year in range(2015, 2021, 1):
        weather_file = os.path.join(DATA_RAW_DIR, 'en_climate_daily_QC_702S006_{0}_P1D.csv'.format(str(year)))
        weather_data = make_weather_data(spark, weather_file)
        weather_data = format_weather_data(weather_data)
        save_data(weather_data, 'weather_{0}.data'.format(str(year)))
    
    spots_file = os.path.join(DATA_RAW_DIR, 'Places.csv')
    spots_data = make_spots_data(spots_file)
    save_spots_data(spots_data, os.path.join(DATA_INTERIM_DIR, 'spots_with_cities.csv'))