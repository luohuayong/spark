import json
import init
sc = init.sc
log = init.log
rd = init.rd
pipe = init.pipe
conn = init.conn
cur = init.cur

def run():
    log.info("user_cart_recommend delete ...")
    sql_del_cart_recommend = "delete from user_cart_recommend"
    cur.execute(sql_del_cart_recommend)
    conn.commit()

    log.info("user_cart_recommend insert ...")
    sql_similarity = "select pid,spid,similarity from product_similarity"
    cur.execute(sql_similarity)
    rdd_product_similarity = sc.parallelize(cur.fetchall())

    sql_cart = "select uid,pid,8 as rating from data_cart"
    cur.execute(sql_cart)
    rdd_cart = sc.parallelize(cur.fetchall())

    import recommend
    #rdd_cart1 = rdd_cart.map(lambda x:(x[0],x[1],8))
    rdd_user_cart_recommend = recommend.recommend(rdd_product_similarity,rdd_cart)
    sql_values = ""
    list_user_cart_recommend = rdd_user_cart_recommend.collect()
    for i in range(len(list_user_cart_recommend)):
        row = list_user_cart_recommend[i]
        sql_values += "(%s,%s,%s)," % row
        if (i != 0 and i % 1000 == 0) or i == len(list_user_cart_recommend) - 1:
            sql_insert = "insert into user_cart_recommend (uid,pid,rating) values %s"\
                % sql_values.rstrip(',')
            sql_values = ""
            cur.execute(sql_insert)
            conn.commit()

    log.info("user_cart_recommend rdd map...")
    list_user_cart_recommend1 = rdd_user_cart_recommend.map(lambda x:(x[0],(x)))\
        .groupByKey().mapValues(list).collect()

    log.info("user_cart_recommend write redis...")
    redis_prefix = "rec.user_cart_recommend.uid"
    for i in range(len(list_user_cart_recommend1)):
        row = list_user_cart_recommend1[i]
        json_value = json.dumps(row[1])
        pipe.set("%s:%s" % (redis_prefix,row[0]),json_value)
    pipe.execute()

if __name__ == "__main__":
    run()


