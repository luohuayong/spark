import init
sc = init.sc
log = init.log
rd = init.rd
pipe = init.pipe
conn = init.conn
cur = init.cur

def run():
    log.info("make rdd_rating...")
    sql_payment = "select uid,pid,0 as rating from data_payment"
    sql_order = "select uid,pid,0 as rating from data_order"
    sql_cart = "select uid,pid,0 as rating from data_cart"
    sql_favorites = "select uid,pid,0 as rating from data_favorites"
    cur.execute(sql_payment)
    rdd_payment = sc.parallelize(cur.fetchall(),10)
    log.info("rdd_payment.count() = %s" % rdd_payment.count())
    cur.execute(sql_order)
    rdd_order = sc.parallelize(cur.fetchall(),10)
    log.info("rdd_order.count() = %s" % rdd_order.count())
    cur.execute(sql_cart)
    rdd_cart = sc.parallelize(cur.fetchall())
    log.info("rdd_cart.count() = %s" % rdd_cart.count())
    cur.execute(sql_favorites)
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
    sql_product = "select pid from data_product where status=1"
    cur.execute(sql_product)
    list_product = cur.fetchall()
    rdd_rating = rdd_rating.filter(lambda x:(x[1],) in list_product)
    log.info("filter rdd_rating.count() = %s" % rdd_rating.count())

    log.info("user_rating delete...")
    sql_del_rating = "delete from user_rating"
    cur.execute(sql_del_rating)
    conn.commit()

    log.info("user_rating insert...")
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

if __name__ == "__main__":
    run()

