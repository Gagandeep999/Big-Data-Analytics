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
RPT_DIR = os.path.join(os.sep.join(PROJ_DIR), 'reports')
RPT_FIGURES_DIR = os.path.join(RPT_DIR, 'figures')

def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('Making final data set from raw data ...')

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


def load_data(filename):
    logging.info('Initiating Spark...')
    spark = init_spark()
    logging.info('Loading data...')
    data = spark.read.csv(filename, header=True)
    return data

def distance_bw_parking_spots(data):
    if not isinstance(data, 'pyspark.sql.dataframe.DataFrame'):
        return
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
        withColumn('Distance(Km)', F.expr('6373.0*C'))
    interim = toCSVLine(data)


if __name__ == '__main__':
    filename = os.path.join(DATA_RAW_DIR, 'remorquages.csv')
    data = load_data(filename)
    distance_bw_parking_spots(data)

    main()
