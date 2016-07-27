# -*- coding:utf-8 -*-
from pyspark.mllib.recommendation import ALS,Rating

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import psycopg2

class RecommendationEngine:
    """The goods recommendation engine
    """


    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        self.model = ALS.train(ratings=self.rdd_ratings,\
                               rank=self.rank,\
                               iterations=self.iterations,\
                               lambda_=self.lambda_,\
                               seed=self.seed)
        logger.info("ALS model built!")

"""
    def __predict_ratings(self,rdd_user_and_goods):
         input for a rdd (user_id,movie_id)
         return: a rdd with format(user_id,goods_id,rating)
        rdd_predicted = self.model.predictAll(rdd_user_and_goods)
        rdd_predicted_rating = add_predicted.map()
"""

    def __init__(self,sc):
        """
         Init recommendation engine for given the spark context
        """
        logger.info("start recommendation engine:")
        self.sc = sc
        logger.info("load rating data...")

        conn = psycopg2.connect(host="121.40.28.12",dbname="bymyhopin_django2",user="odoo",password="bysun123")
        sql_rating = "select uid,pid,rating from user_rating"
        cur = conn.cursor()
        cur.execute(sql_rating)
        rawdata = sc.parallelize(cur.fetchall())
        user_num = rawdata.map(lambda x:(x[0])).distinct().count()
        product_num = rawdata.map(lambda x:(x[1])).distinct().count()
        self.rdd_ratings = rawdata.map(lambda x:Rating(int(x[0]),int(x[1]),float(x[2]))).cache()
        logger.info("loaded %s rows rating data for %s users and %s products " \
                    % (rawdata.count(),user_num,product_num))

        #the model param for train
        self.rank = 50
        self.seed = 5L
        self.iterations = 10
        self.lambda_ = 0.1
        self.__train_model()

    def get_top_ratings(user_id,count):
        """
         recommendation up to count top products to user_id
        """
        logger.info("call get_top_ratings(%s,%s)" % (user_id,count))
        ratings = self.model.recommendProducts(user_id,count)
        return ratings


