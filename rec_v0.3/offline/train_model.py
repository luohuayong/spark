from pyspark.mllib.recommendation import ALS,Rating
import init
sc = init.sc
log = init.log
rd = init.rd
pipe = init.pipe
conn = init.conn
cur = init.cur

def run():
    log.info("train model start...")
    sql_user_rating = "select uid,pid,rating from user_rating"
    cur.execute(sql_user_rating)
    rdd_user_rating = sc.parallelize(cur.fetchall())

    ratings = rdd_user_rating.map(lambda x:Rating(int(x[0]),int(x[1]),float(x[2])))
    mod = ALS.train(ratings,50)
    return mod

if __name__ == "__main__":
    run()

