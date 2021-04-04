import os
import logging
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)

PROJ_DIR = os.getcwd().split(os.sep)[:-2]
DATA_DIR = os.path.join(os.sep.join(PROJ_DIR), 'data')
DATA_RAW_DIR = os.path.join(DATA_DIR, 'raw')
DATA_INTERIM_DIR = os.path.join(DATA_DIR, 'interim')


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


def load_data(spark, filename):
    logging.info('Loading data...')
    data = spark.read.csv(filename, header=True)
    return data


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


def combine_towing_and_weather(towing_data, weather_data):
    logging.info('Joinging the towing and weather data to make final dataset ...')
    pass


def make_towing_data(spark, filename):
    logging.info('Making towing data ...')
    data = load_data(spark, filename)
    return data


def make_weather_data(spark, filename):
    logging.info('Making weather data ...')
    data = load_data(spark, filename)
    return data


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


def save_towing_data(data):
    if not isinstance(data, DataFrame):
        raise ValueError('Cannot save towing data. It is not a PySpark DataFrame object')
    logging.info('Saving towing data as parquet file to ../interim directory')
    data.write.mode('overwrite').parquet(os.path.join(DATA_INTERIM_DIR, 'towing.data'))


def save_weather_data(data):
    if not isinstance(data, DataFrame):
        raise ValueError('Cannot save towing data. It is not a PySpark DataFrame object')
    logging.info('Saving weather data as parquet file to ../interim directory')
    data.write.mode('overwrite').parquet(os.path.join(DATA_INTERIM_DIR, 'weather.data'))


if __name__ == '__main__':
    towing_file = os.path.join(DATA_RAW_DIR, 'remorquages.csv')
    weather_file = os.path.join(DATA_RAW_DIR, 'en_climate_daily_QC_702S006_2015_P1D.csv')

    logging.info('Initiating Spark...')
    spark = init_spark()

    towing_data = make_towing_data(spark, towing_file)
    towing_data = format_towing_data(towing_data)
    weather_data = make_weather_data(spark, weather_file)
    weather_data = format_weather_data(weather_data)
    save_weather_data(weather_data)
    towing_with_distance = distance_bw_parking_spots(towing_data)
    save_towing_data(towing_with_distance)
    combine_towing_and_weather(towing_data, weather_data)


    # spark_session, data = load_data(filename)
    # spark_session, data = distance_bw_parking_spots(spark_session, data)
    # combine_with_weather(spark_session, data)