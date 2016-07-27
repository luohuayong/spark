from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS,Rating
import psycopg2
import time
sc = SparkContext("local[2]","spark_test3")

conn = psycopg2.connect(host="121.40.28.12",dbname="bymyhopin_django2",user="odoo",password="bysun123")
sql_rating = "select uid,pid,rating from user_rating"
cur = conn.cursor()
cur.execute(sql_rating)
rawdata = sc.parallelize(cur.fetchall())
print "rawdata.count() = %s" % rawdata.count()
rdd_user = rawdata.map(lambda x:(x[0])).distinct()
users = sorted(rdd_user.collect())
print "users.count() = %s" % len(users)
rdd_product = rawdata.map(lambda x:(x[1])).distinct()
products = sorted(rdd_product.collect())
print "products.count() = %s" % len(products)
ratings = rawdata.map(lambda x:Rating(int(x[0]),int(x[1]),float(x[2])))
model = ALS.train(ratings,50)
collect = model.recommendProductsForUsers(30).collect()
print "collect.count() = %s" % len(collect)
for i in range(len(collect)):
    for j in range(len(collect[i][1])):
        row = collect[i][1][j]
        sql_insert = "insert into user_recommend (uid,pid,rating) values (%s,%s,%s)" % (row.user,row.product,row.rating)
        cur.execute(sql_insert)
    if i != 0 and i % 100 == 0:
        print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        conn.commit()
        print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print "commit users %s" % i
conn.commit()
cur.close()
conn.close()
sc.stop()


#conn = psycopg2.connect(dbname="sparktest",user="leo",password="123123")
#cur = conn.cursor()
#sql = "select * from user_rating;"
#cur.execute(sql)
#print cur.fetchone()

