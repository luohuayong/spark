from pyspark import SparkContext
#from pyspark.mllib.recommendation import ALS,Rating
from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka import KafkaUtils
import psycopg2
import logging
#logging.basicConfig(level=logging.DEBUG)
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
#import os
import ConfigParser
cf = ConfigParser.ConfigParser()
conf_path = "/mnt/conf/engine.conf"
cf.read(conf_path)
db_host = cf.get("database","host")
db_dbname = cf.get("database","dbname")
db_user = cf.get("database","user")
db_pwd = cf.get("database","pwd")

sc = SparkContext(appName="engine")
ssc = StreamingContext(sc,10)

#rdd_queue = []
#for i in range(5):
#    rdd_queue += [ssc.sparkContext.parallelize([j for j in range(1,1001)],10)]
#inputStream = ssc.queueStream(rdd_queue)
#mappedStream = inputStream.map(lambda x:(x % 10,1))
#reducedStream = mappedStream.reduceByKey(lambda a, b: a + b)
#reducedStream.pprint()
#ssc.start()
#time.sleep(60)
#ssc.stop(stopSparkContext=True,stopGraceFully=True)

conn = psycopg2.connect(host=db_host,dbname=db_dbname,user=db_user,password=db_pwd)
cur = conn.cursor()
log.info("make rdd_product_similarity...")
sql_similarity = "select pid,spid,similarity from product_similarity"
cur.execute(sql_similarity)
rdd_product_similarity = sc.parallelize(cur.fetchall())

serverIP = "127.0.0.1"
serverPort = 9999
ds_lines = ssc.socketTextStream(serverIP,serverPort)
ds_rating = ds_lines.map(lambda line:(line.split(",")))


#rdd_rating1 = ssc.sparkContext.parallelize(["61,189254,3.0"])
#rdd_rating2 = ssc.sparkContext.parallelize(["61,189254,5.0"])
#rdd_rating3 = ssc.sparkContext.parallelize(["61,189151,3.0"])
#rdd_rating4 = ssc.sparkContext.parallelize(["61,189151,7.0"])
#rdd_rating5 = ssc.sparkContext.parallelize(["61,189083,3.0"])
#rdd_queue = [rdd_rating1,rdd_rating2,rdd_rating3,rdd_rating4,rdd_rating5]
#inputStream = ssc.queueStream(rdd_queue)

ssc.start()
ssc.awaitTermination()
