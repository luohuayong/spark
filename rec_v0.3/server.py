from pyspark import SparkContext,SparkConf

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_spark_context():
    conf = SparkConf().setAppName("recommendation-server")
    sc = SparkContext(conf=conf,pyFiles=['engine.py','app.py'])
    return sc

#def run_server(app):

