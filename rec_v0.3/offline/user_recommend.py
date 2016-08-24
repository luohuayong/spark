import json
import init
sc = init.sc
log = init.log
rd = init.rd
pipe = init.pipe
conn = init.conn
cur = init.cur

def run(mod):
    log.info("delete user_recommend ...")
    sql_del_recommend = "delete from user_recommend"
    cur.execute(sql_del_recommend)
    conn.commit()

    log.info("user_recommend insert...")
    list_recommend = mod.recommendProductsForUsers(30).collect()
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

    log.info("user_recommend write redis...")
    redis_prefix = "rec.user_recommend.uid"
    for i in range(len(list_recommend)):
        row = list_recommend[i]
        json_value = json.dumps(row[1])
        pipe.set("%s:%s" % (redis_prefix,row[0]),json_value)
    pipe.execute()

if __name__ == "__main__":
    import train_model
    mod = train_model.run()
    run(mod)


