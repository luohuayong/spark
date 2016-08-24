import json
import init
sc = init.sc
log = init.log
rd = init.rd
pipe = init.pipe
conn = init.conn
cur = init.cur

def run():
    log.info("delete product_hot ...")
    sql_del_hot = "delete from product_hot"
    cur.execute(sql_del_hot)
    conn.commit()

    log.info("product_hot insert ...")
    sql_user_rating = "select uid,pid,rating from user_rating"
    cur.execute(sql_user_rating)
    rdd_user_rating = sc.parallelize(cur.fetchall())

    rdd_hot = rdd_user_rating.map(lambda x:(x[1],x[2]))\
        .reduceByKey(lambda x,y:x+y)\
        .sortBy(lambda x:x[1],False)
    list_hot = rdd_hot.take(30)

    sql_values = ""
    for i in range(len(list_hot)):
        sql_values += "(%s,%s)," % list_hot[i]
    sql_insert = "insert into product_hot (pid,hot) values %s" \
        % sql_values.rstrip(',')
    cur.execute(sql_insert)
    conn.commit()

    log.info("product_hot write redis...")
    redis_prefix = "rec.product_hot"
    json_value = json.dumps(list_hot)
    rd.set(redis_prefix,json_value)

if __name__ == "__main__":
    run()
