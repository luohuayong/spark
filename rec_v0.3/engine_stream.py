#import pdb
#pdb.set_trace()
#import pudb;pu.db
import ConfigParser
cf = ConfigParser.ConfigParser()
conf_path = "/mnt/conf/engine.conf"
cf.read(conf_path)

from pyspark import SparkContext
sc = SparkContext(appName="engine")

from pyspark.streaming import StreamingContext
ssc = StreamingContext(sc,10)

import logging
import logging.config
import json
with open("/mnt/src/log.json",'rt') as f:
    config = json.load(f)
logging.config.dictConfig(config)
log = logging.getLogger(__name__)

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

log.info("make rdd_product_similarity...")
sql_similarity = "select pid,spid,similarity from product_similarity"
cur.execute(sql_similarity)
rdd_product_similarity = sc.parallelize(cur.fetchall())

'''
 items_similar [(pid,rpid,similarity)] rdd
 user_prefer [(uid,pid,rating)] rdd
'''
def Recommend(items_similar,user_prefer):

    def map_fun1(f):
        i2_2 = sorted(f[1],reverse=True,key=lambda x:x[1])
        #if len(i2_2) > r_number :
        #    i2_2.remove(0,(len(i2_2)-r_number))
        return (f[0],i2_2)

    def map_fun2(f):
        r = []
        for w in f[1]:
            r.append((f[0],w[0],w[1]))
        return r

    #rdd_app1_R2 [(pid,((rpid,similarity),(uid,rating)))]
    rdd_app1_R2 = items_similar.map(lambda f:(f[0],(f[1],f[2])))\
        .join(user_prefer.map(lambda f:(f[1],(f[0],f[2]))))
    #rdd_app1_R3 [(uid,rpid),rating*similarity]
    rdd_app1_R3 = rdd_app1_R2.map(lambda f:((f[1][1][0],f[1][0][0]),f[1][1][1]*f[1][0][1]))
    #rdd_app1_R4 [(uid,rpid),sum(rating*similarity)]
    #rdd_app1_R4 [(uid,(rpid,sum(rating*similarity)))]
    rdd_app1_R4 = rdd_app1_R3.reduceByKey(lambda x,y:x+y)\
        .map(lambda f:(f[0][0],(f[0][1],f[1])))
    #rdd_app1_R5 [(uid,((rpid,sum(rating*similarity))...))]
    rdd_app1_R5 = rdd_app1_R4.groupByKey()
    #rdd_app1_R6 [(uid,sorted((rpid,sum(rating*similarity))...))]
    rdd_app1_R6 = rdd_app1_R5.map(lambda x:map_fun1(x))
    #rdd_app1_R7 [(uid,rpid,sum(rating*similarity))]
    rdd_app1_R7 = rdd_app1_R6.flatMap(lambda x:map_fun2(x))
#    rdd_app1_R7.map(lambda f:f[0]).distinct().count()
    return rdd_app1_R7


from pykafka import KafkaClient
ka_hosts = cf.get("kafka","kafka_hosts")
zp_hosts = cf.get("kafka","zookeeper_hosts")
ka_topics = cf.get("kafka","topics")
ka_group = cf.get("kafka","group")
client = KafkaClient(hosts=ka_hosts)
topic = client.topics[ka_topics]
consumer = topic.get_simple_consumer(consumer_group=ka_group)
log.info("subscribe kafka[%s] topic[%s] group[%s]:" % \
    (ka_hosts,ka_topics,ka_group))
while True:
    message = consumer.consume()
    consumer.commit_offsets()
    log.info(message.value)
    row = message.value.split(',')
    rdd1 = sc.parallelize([(int(row[0]),int(row[1]),float(row[2]))])
    rdd2 = Recommend(rdd_product_similarity,rdd1)
    list_rdd2 = rdd2.collect()
    log.info(list_rdd2)

    log.info("redis write start ...")
    redis_prefix = "rec.user_recommend.uid"
    json_value = json.dumps(list_rdd2)
    rd.set("%s:%s" % (redis_prefix,list_rdd2[0][0]),json_value)



#for message in consumer:
#    if message is not None:
#        log.info(message.value)
#        row = message.value.split(',')
#        rdd1 = sc.parallelize([(int(row[0]),int(row[1]),float(row[2]))])
#        rdd2 = Recommend(rdd_product_similarity,rdd1)
#        log.info(rdd2.collect())




#from pyspark.streaming.kafka import KafkaUtils
#ka_hosts = cf.get("kafka","kafka_hosts")
#zp_hosts = cf.get("kafka","zookeeper_hosts")
#ka_topics = cf.get("kafka","topics")
#ka_group = cf.get("kafka","group")
#kvs = KafkaUtils.createStream(ssc,zp_hosts,ka_group,{ka_topics:1})
#kvs.pprint()

#ssc.start()
#ssc.awaitTermination()

#serverIP = "127.0.0.1"
#serverPort = 9999
#rdd1 = sc.parallelize([(61,189254,3.0)])
#rdd2 = Recommend(rdd_product_similarity,rdd1)
#print (rdd2.collect())

#ds_lines = ssc.socketTextStream(serverIP,serverPort)
#ds_rating = ds_lines.map(lambda line:(line.split(",")))
#ds_rating.transform(lambda rdd:Recommend(rdd_product_similarity,rdd)).pprint()
#ds_rating.pprint()
#rdd_rating1 = ssc.sparkContext.parallelize(["61,189254,3.0"])
#rdd_rating2 = ssc.sparkContext.parallelize(["61,189254,5.0"])
#rdd_rating3 = ssc.sparkContext.parallelize(["61,189151,3.0"])
#rdd_rating4 = ssc.sparkContext.parallelize(["61,189151,7.0"])
#rdd_rating5 = ssc.sparkContext.parallelize(["61,189083,3.0"])
#rdd_queue = [rdd_rating1,rdd_rating2,rdd_rating3,rdd_rating4,rdd_rating5]
#inputStream = ssc.queueStream(rdd_queue)

