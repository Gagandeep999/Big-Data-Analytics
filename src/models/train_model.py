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


def save_model(model, save_dir):
    model.save(save_dir)
    

def train_model(train_data):
    if not isinstance(train_data, DataFrame):
        raise ValueError('Param to train_model.preprocess_data() should be DataFrame.')
    lr = LinearRegression(featuresCol = 'features', labelCol='Distance_km')
    lr_model = lr.fit(train_data)
    trainSummary = lr_model.summary
    logging.info("RMSE: %f" % trainSummary.rootMeanSquaredError)
    logging.info("\nr2: %f" % trainSummary.r2)
    return lr_model


def split_data(data):
    if not isinstance(data, DataFrame):
        raise ValueError('Param to train_model.preprocess_data() should be DataFrame.')
    train,test = data.randomSplit([0.70, 0.30])
    return (train, test)


def preprocess_data_for_training(data):
    if not isinstance(data, DataFrame):
        raise ValueError('Param to train_model.preprocess_data() should be DataFrame.')
    features = data.drop('DATE_ORIGINE','Distance_km','MOTIF_REMORQUAGE','Date_Time','Year', 'Spd_of_Max_Gust')
    
    try:
        for col_name in features.schema.names:
            if features.filter(features[col_name].isNull()).count() > 0:
                raise AssertionError
    except AssertionError:
        logging.error('Error in train_model.preprocess_data()')

    vectorAssembler = VectorAssembler(inputCols = features.columns, outputCol = 'features')
    training_df = vectorAssembler.transform(data)
    training_df = training_df.select(['features', 'Distance_km'])

    return training_df


if __name__ == '__main__':
    spark = make_dataset.init_spark()

    cleaned_file = os.path.join(DATA_PROCESSED_DIR, 'cleaned.data')
    cleaned_data = make_dataset.load_data_parquet(spark, cleaned_file)

    training_data = preprocess_data_for_training(cleaned_data)
    train_data, test_data = split_data(training_data)

    model = train_model(train_data)

    today = datetime.today().strftime('%Y-%m-%d')
    model_file = os.path.join(MODEL_DIR, today)
    save_model(model, model_file)