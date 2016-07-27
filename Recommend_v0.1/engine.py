from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS,Rating
import psycopg2
sc = SparkContext(appName="engine")

import logging
#logging.basicConfig(level=logging.DEBUG)
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


conn = psycopg2.connect(host="121.40.28.12",dbname="bymyhopin_django2",user="odoo",password="bysun123")
cur = conn.cursor()
log.info("make rdd_rating...")
sql_payment = "select uid,pid,0 as rating from data_payment"
sql_order = "select uid,pid,0 as rating from data_order"
sql_cart = "select uid,pid,0 as rating from data_cart"
sql_favorites = "select uid,pid,0 as rating from data_favorites"
cur.execute(sql_payment)
rdd_payment = sc.parallelize(cur.fetchall())
log.debug("rdd_payment.count() = %s" % rdd_payment.count())
cur.execute(sql_order)
rdd_order = sc.parallelize(cur.fetchall())
log.debug("rdd_order.count() = %s" % rdd_order.count())
data_cart = cur.execute(sql_cart)
rdd_cart = sc.parallelize(cur.fetchall())
log.debug("rdd_cart.count() = %s" % rdd_cart.count())
data_favorites = cur.execute(sql_favorites)
rdd_favorites = sc.parallelize(cur.fetchall())
log.debug("rdd_favorites.count() = %s" % rdd_favorites.count())
rdd_rating = sc.emptyRDD()
rdd_temp = rdd_payment.map(lambda x:(x[0],x[1],10.0))
rdd_rating = rdd_rating.union(rdd_temp)
log.debug("rdd_temp_payment.count() = %s" % rdd_temp.count())
rdd_temp = rdd_order.subtract(rdd_payment).map(lambda x:(x[0],x[1],8.0))
rdd_rating = rdd_rating.union(rdd_temp)
log.debug("rdd_temp_order.count() = %s" % rdd_temp.count())
rdd_temp = rdd_cart.subtract(rdd_order).subtract(rdd_payment).map(lambda x:(x[0],x[1],7.0))
rdd_rating = rdd_rating.union(rdd_temp)
log.debug("rdd_temp_cart.count() = %s" % rdd_temp.count())
rdd_temp = rdd_favorites.subtract(rdd_cart).subtract(rdd_order).subtract(rdd_payment).map(lambda x:(x[0],x[1],5.0))
rdd_rating = rdd_rating.union(rdd_temp)
log.debug("rdd_temp_favorites.count() = %s" % rdd_temp.count())
log.info("rdd_rating.count() = %s" % rdd_rating.count())
collect = rdd_rating.collect()
log.info("rdd_rating complete!")

sql_del_rating = "delete from user_rating"
cur.execute(sql_del_rating)
conn.commit()
log.info("user_rating delete")

log.info("user_rating insert start...")
sql_values = ""
for i in range(len(collect)):
    sql_values += "(%s,%s,%s)," % collect[i]
    if (i != 0 and i % 1000 == 0) or i == len(collect)-1:
        sql_insert = "insert into user_rating (uid,pid,rating) values %s" \
            % sql_values.rstrip(',')
        sql_values = ""
        cur.execute(sql_insert)
        conn.commit()
log.info("user_rating(%s rows) insert complete!" % len(collect))
#sql_rating = "select uid,pid,rating from user_rating"
#cur.execute(sql_rating)
#rawdata = sc.parallelize(cur.fetchall())
#log.debug("rawdata.count() = %s" % rawdata.count())
#rdd_user = rawdata.map(lambda x:(x[0])).distinct()
#users = sorted(rdd_user.collect())
#log.debug("users.count() = %s" % len(users))
#rdd_product = rawdata.map(lambda x:(x[1])).distinct()
#products = sorted(rdd_product.collect())
#log.debug("products.count() = %s" % len(products))
#ratings = rawdata.map(lambda x:Rating(int(x[0]),int(x[1]),float(x[2])))

log.info("train model start...")
ratings = rdd_rating.map(lambda x:Rating(int(x[0]),int(x[1]),float(x[2])))
model = ALS.train(ratings,50)
collect = model.recommendProductsForUsers(30).collect()
#log.debug("collect.count() = %s" % len(collect))
log.info("train model complete!")

sql_del_recommend = "delete from user_recommend"
cur.execute(sql_del_recommend)
conn.commit()
log.info("user_recommend delete")

log.info("user_recommend insert start...")
sql_values = ""
rows_num = 0
for i in range(len(collect)):
    for j in range(len(collect[i][1])):
        row = collect[i][1][j]
        sql_values += "(%s,%s,%s)," % (row.user,row.product,row.rating)
        rows_num += 1
    if (i != 0 and i % 100 == 0) or i == len(collect)-1:
        sql_insert = "insert into user_recommend (uid,pid,rating) values %s" % sql_values.rstrip(',')
        sql_values = ""
        cur.execute(sql_insert)
        conn.commit()
log.info("user_recommend(%s users and %s rows) insert complete!" % (len(collect),rows_num))

sql_del_recommend = "delete from user_recommend_bak"
cur.execute(sql_del_recommend)
conn.commit()
log.info("user_recommend_bak delete")

log.info("user_recommend_bak insert start...")
sql_values = ""
rows_num = 0
for i in range(len(collect)):
    for j in range(len(collect[i][1])):
        row = collect[i][1][j]
        sql_values += "(%s,%s,%s)," % (row.user,row.product,row.rating)
        rows_num += 1
    if (i != 0 and i % 100 == 0) or i == len(collect)-1:
        sql_insert = "insert into user_recommend_bak (uid,pid,rating) values %s" % sql_values.rstrip(',')
        sql_values = ""
        cur.execute(sql_insert)
        conn.commit()
log.info("user_recommend_bak(%s users and %s rows) insert complete!" % (len(collect),rows_num))

cur.close()
conn.close()
sc.stop()


#conn = psycopg2.connect(dbname="sparktest",user="leo",password="123123")
#cur = conn.cursor()
#sql = "select * from user_rating;"
#cur.execute(sql)
#print cur.fetchone()

