from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS,Rating
import numpy as np
sc = SparkContext(appName="engine")

import logging
import logging.config
import json
with open("/mnt/src/log.json",'rt') as f:
    config = json.load(f)
logging.config.dictConfig(config)
log = logging.getLogger(__name__)

#import os
import ConfigParser
cf = ConfigParser.ConfigParser()
conf_path = "/mnt/conf/engine.conf"
cf.read(conf_path)

import redis
rd_host = cf.get("redis","host")
rd_port = cf.get("redis","port")
rd_pwd = cf.get("redis","pwd")
rd = redis.Redis(host=rd_host,port=rd_port,db=0,password=rd_pwd)

import psycopg2
db_host = cf.get("database","host")
db_dbname = cf.get("database","dbname")
db_user = cf.get("database","user")
db_pwd = cf.get("database","pwd")
conn = psycopg2.connect(host=db_host,dbname=db_dbname,user=db_user,password=db_pwd)
cur = conn.cursor()

#log.info("load data_product ...")
#sql_product = "select pid,status from data_product where status=1"
#cur.execute(sql_product)
#rows = cur.fetchall()
#dict_data_product = {}
#for row in rows:
#    dict_data_product[str(row[0])]=1


log.info("make rdd_rating...")
sql_payment = "select uid,pid,0 as rating from data_payment"
sql_order = "select uid,pid,0 as rating from data_order"
sql_cart = "select uid,pid,0 as rating from data_cart"
sql_favorites = "select uid,pid,0 as rating from data_favorites"
cur.execute(sql_payment)
rdd_payment = sc.parallelize(cur.fetchall())
log.info("rdd_payment.count() = %s" % rdd_payment.count())
cur.execute(sql_order)
rdd_order = sc.parallelize(cur.fetchall())
log.info("rdd_order.count() = %s" % rdd_order.count())
data_cart = cur.execute(sql_cart)
rdd_cart = sc.parallelize(cur.fetchall())
log.info("rdd_cart.count() = %s" % rdd_cart.count())
data_favorites = cur.execute(sql_favorites)
rdd_favorites = sc.parallelize(cur.fetchall())
log.info("rdd_favorites.count() = %s" % rdd_favorites.count())
rdd_rating = sc.emptyRDD()
rdd_temp = rdd_payment.map(lambda x:(x[0],x[1],10.0))
rdd_rating = rdd_rating.union(rdd_temp)
log.info("rdd_temp_payment.count() = %s" % rdd_temp.count())
rdd_temp = rdd_order.subtract(rdd_payment).map(lambda x:(x[0],x[1],8.0))
rdd_rating = rdd_rating.union(rdd_temp)
log.info("rdd_temp_order.count() = %s" % rdd_temp.count())
rdd_temp = rdd_cart.subtract(rdd_order).subtract(rdd_payment).map(lambda x:(x[0],x[1],7.0))
rdd_rating = rdd_rating.union(rdd_temp)
log.info("rdd_temp_cart.count() = %s" % rdd_temp.count())
rdd_temp = rdd_favorites.subtract(rdd_cart).subtract(rdd_order).subtract(rdd_payment).map(lambda x:(x[0],x[1],5.0))
rdd_rating = rdd_rating.union(rdd_temp)
log.info("rdd_temp_favorites.count() = %s" % rdd_temp.count())
log.info("rdd_rating.count() = %s" % rdd_rating.count())
#log.info("rdd_rating complete!")

log.info("delete user_rating ...")
sql_del_rating = "delete from user_rating"
cur.execute(sql_del_rating)
conn.commit()

log.info("user_rating insert start...")
list_rating = rdd_rating.collect()
sql_values = ""
for i in range(len(list_rating)):
    sql_values += "(%s,%s,%s)," % list_rating[i]
    if (i != 0 and i % 1000 == 0) or i == len(list_rating)-1:
        sql_insert = "insert into user_rating (uid,pid,rating) values %s" \
            % sql_values.rstrip(',')
        sql_values = ""
        cur.execute(sql_insert)
        conn.commit()
log.info("user_rating(%s rows) insert complete!" % len(list_rating))

log.info("train model start...")
ratings = rdd_rating.map(lambda x:Rating(int(x[0]),int(x[1]),float(x[2])))
model = ALS.train(ratings,50)
#log.debug("collect.count() = %s" % len(collect))
#log.info("train model complete!")

log.info("delete user_recommend ...")
sql_del_recommend = "delete from user_recommend"
cur.execute(sql_del_recommend)
conn.commit()

log.info("user_recommend insert start...")
list_recommend = model.recommendProductsForUsers(30).collect()

#for i in range(len(list_recommend)):
#    list_item = sc.parallelize(list_recommend[i][1])\
#        .filter(lambda x:dict_data_product.has_key(str(x[1])))\
#        .take(30)
#    list_recommend[i]=list_item


sql_values = ""
rows_num = 0
for i in range(len(list_recommend)):
    for j in range(len(list_recommend[i][1])):
        row = list_recommend[i][1][j]
        sql_values += "(%s,%s,%s)," % (row.user,row.product,row.rating)
        rows_num += 1
    if (i != 0 and i % 100 == 0) or i == len(list_recommend)-1:
        sql_insert = "insert into user_recommend (uid,pid,rating) values %s" % sql_values.rstrip(',')
        sql_values = ""
        cur.execute(sql_insert)
        conn.commit()
log.info("user_recommend(%s users and %s rows) insert complete!" % (len(list_recommend),rows_num))

def cosineSimilarity(x,y):
    return np.dot(x,y)/(np.linalg.norm(x)*np.linalg.norm(y))

log.info("product_similarity delete start ...")
sql_del_product_similarity = "delete from product_similarity"
cur.execute(sql_del_product_similarity)
conn.commit()

log.info("product_similarity insert start...")
rdd_feature = model.productFeatures()#.filter(lambda x:dict_data_product.has_key[str(x[0])])
#log.info("rdd_feature.take(1)=%s".rdd_feature.take(1))
rdd_feature.cache()
list_feature = rdd_feature.collect()
sql_values = ""
rows_num = 0
list_product_similarity = []
for i in range(len(list_feature)):
#for i in range(10):

    pid,pFactor = list_feature[i]
    #log.info("info 1")
    sims = rdd_feature.map(lambda (id,factor):(id,cosineSimilarity(factor,pFactor)))
    #log.info("info 2 -- sims.count() = %s" % sims.count())
    # slow begin
    sims.cache()
    item_similarity = sims.sortBy((lambda x:x[1]),ascending = False)\
        .filter(lambda x:x[0] != pid).take(30)
    # slow end
    #log.info("info 3")
    list_product_similarity.append((pid,item_similarity))
    #print(list_product_similarity)
    #log.info("info 4")
    for j in range(len(item_similarity)):
        row = item_similarity[j]
        sql_values += "(%s,%s,%s)," % (pid,row[0],row[1])
        rows_num += 1
       # log.info("rows_num = %s" % rows_num)
    #log.info("info 5")
    if (i != 0 and i % 100 == 0) or i == len(list_feature) - 1:
        sql_insert = "insert into product_similarity (pid,spid,similarity) values %s"\
            % sql_values.rstrip(',')
        sql_values = ""
        cur.execute(sql_insert)
        conn.commit()
log.info("product_similarity(%s products and %s rows) insert complete!" % (len(list_feature),rows_num))

#log.info("redis write start ...")
#redis_prefix = "rec.user_recommend.uid"
#for i in range(len(list_recommend)):
#    row = list_recommend[i]
#    json_value = json.dumps(row[1])
#    rd.set("%s:%s" % (redis_prefix,row[0]),json_value)
#log.info("redis rec.user_recommend(%s users) insert complete!" % len(list_recommend))

#redis_prefix = "rec.product_similarity.pid"
#for i in range(len(list_product_similarity)):
#    row = list_product_similarity[i]
#    json_value = json.dumps(row[1])
#    rd.set("%s:%s" % (redis_prefix,row[0]),json_value)
#log.info("redis rec.product_similarity(%s products) insert complete!" % len(list_product_similarity))

cur.close()
conn.close()
sc.stop()


#conn = psycopg2.connect(dbname="sparktest",user="leo",password="123123")
#cur = conn.cursor()
#sql = "select * from user_rating;"
#cur.execute(sql)
#print cur.fetchone()

