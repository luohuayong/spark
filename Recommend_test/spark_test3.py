from pyspark import SparkContext

import psycopg2
sc = SparkContext("local[2]","spark_test3")

conn = psycopg2.connect(host="121.40.28.12",dbname="bymyhopin_django2",user="odoo",password="bysun123")
sql_payment = "select uid,pid,0 as rating from data_payment"
sql_order = "select uid,pid,0 as rating from data_order"
sql_cart = "select uid,pid,0 as rating from data_cart"
sql_favorites = "select uid,pid,0 as rating from data_favorites"
cur = conn.cursor()
cur.execute(sql_payment)
rdd_payment = sc.parallelize(cur.fetchall())
print "rdd_payment.count() = %s" % rdd_payment.count()
cur.execute(sql_order)
rdd_order = sc.parallelize(cur.fetchall())
print "rdd_order.count() = %s" % rdd_order.count()
data_cart = cur.execute(sql_cart)
rdd_cart = sc.parallelize(cur.fetchall())
print "rdd_cart.count() = %s" % rdd_cart.count()
data_favorites = cur.execute(sql_favorites)
rdd_favorites = sc.parallelize(cur.fetchall())
print "rdd_favorites.count() = %s" % rdd_favorites.count()
rdd_rating = sc.emptyRDD()
rdd_temp = rdd_payment.map(lambda x:(x[0],x[1],10.0))
rdd_rating = rdd_rating.union(rdd_temp)
print "rdd_temp_payment.count() = %s" % rdd_temp.count()
rdd_temp = rdd_order.subtract(rdd_payment).map(lambda x:(x[0],x[1],8.0))
rdd_rating = rdd_rating.union(rdd_temp)
print "rdd_temp_order.count() = %s" % rdd_temp.count()
rdd_temp = rdd_cart.subtract(rdd_order).subtract(rdd_payment).map(lambda x:(x[0],x[1],7.0))
rdd_rating = rdd_rating.union(rdd_temp)
print "rdd_temp_cart.count() = %s" % rdd_temp.count()
rdd_temp = rdd_favorites.subtract(rdd_cart).subtract(rdd_order).subtract(rdd_payment).map(lambda x:(x[0],x[1],5.0))
rdd_rating = rdd_rating.union(rdd_temp)
print "rdd_temp_favorites.count() = %s" % rdd_temp.count()
print "rdd_rating.count() = %s" % rdd_rating.count()

collect = rdd_rating.collect()
try:
    for i in range(len(collect)):
        sql_insert = "insert into user_rating (uid,pid,rating) values (%s,%s,%s)" % collect[i]
        cur.execute(sql_insert)
        if i != 0 and i % 1000 == 0:
            conn.commit()
            print "commit count %s" % i
except:
    print collect[i]
    raise
finally:
    conn.close()
conn.commit()
cur.close()
conn.close()



#conn = psycopg2.connect(dbname="sparktest",user="leo",password="123123")
#cur = conn.cursor()
#sql = "select * from user_rating;"
#cur.execute(sql)
#print cur.fetchone()

