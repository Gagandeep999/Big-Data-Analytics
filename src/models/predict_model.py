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

PROJ_DIR = os.getcwd().split(os.sep)[:-2]
sys.path.append(os.sep.join(PROJ_DIR))
from src.data import make_dataset

DATA_DIR = os.path.join(os.sep.join(PROJ_DIR), 'data')
DATA_RAW_DIR = os.path.join(DATA_DIR, 'raw')
DATA_INTERIM_DIR = os.path.join(DATA_DIR, 'interim')
DATA_PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
MODEL_DIR = os.path.join(os.sep.join(PROJ_DIR), 'models')


log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)


def load_model():
    return


def predict():
    pass


if __name__ == '__main__':
    spark = make_dataset.init_spark()
    model = load_model()