from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS,Rating
import numpy as np
import psycopg2
sc = SparkContext(appName="engine")

import logging
#logging.basicConfig(level=logging.DEBUG)
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

def cosineSimilarity(x,y):
    return np.dot(x,y)/(np.linalg.norm(x)*np.linalg.norm(y))

import os
import ConfigParser
cf = ConfigParser.ConfigParser()
conf_path = "/mnt/conf/engine.conf"
cf.read(conf_path)
db_host = cf.get("database","host")
db_dbname = cf.get("database","dbname")
db_user = cf.get("database","user")
db_pwd = cf.get("database","pwd")

conn = psycopg2.connect(host=db_host,dbname=db_dbname,user=db_user,password=db_pwd)
cur = conn.cursor()
sql_rating = "select uid,pid,rating from user_rating"
cur.execute(sql_rating)
rdd_rating = sc.parallelize(cur.fetchall())

log.info("train model start...")
ratings = rdd_rating.map(lambda x:Rating(int(x[0]),int(x[1]),float(x[2])))
model = ALS.train(ratings,50)
log.info("train model complete!")

sql_del_product_similarity = "delete from product_similarity"
cur.execute(sql_del_product_similarity)
conn.commit()
log.info("product_similarity delete")

log.info("product_similarity insert start...")
rdd_feature = model.productFeatures()
rdd_feature.cache()
list_feature = rdd_feature.collect()
#product_num = feature_collect.count()
sql_values = ""
rows_num = 0
for i in range(len(list_feature)):
    pid,pFactor = list_feature[i]
#    pid = item[0]
#    pFactor = item[1]
    sims = rdd_feature.map(lambda (id,factor):(id,cosineSimilarity(factor,pFactor)))
    item_similarity = sims.sortBy((lambda x:x[1]),ascending = False)\
        .filter(lambda x:x[0] != pid).take(30)
    for j in range(len(item_similarity)):
        row = item_similarity[j]
        sql_values += "(%s,%s,%s)," % (pid,row[0],row[1])
        rows_num += 1
    if (i != 0 and i % 100 == 0) or i == len(list_feature) - 1:
        sql_insert = "insert into product_similarity (pid,spid,similarity) values %s"\
            % sql_values.rstrip(',')
        sql_values = ""
        cur.execute(sql_insert)
        conn.commit()
log.info("product_similarity(%s products and %s rows) insert complete!" % (len(list_feature),rows_num))


cur.close()
conn.close()
sc.stop()


