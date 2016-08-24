from pyspark import SparkContext
sc = SparkContext(appName="rec.engine.model")

import logging
import logging.config
import json
with open("/mnt/src/log.json",'rt') as f:
    config = json.load(f)
logging.config.dictConfig(config)
log = logging.getLogger(__name__)

import ConfigParser
cf = ConfigParser.ConfigParser()
conf_path = "/mnt/conf/engine.conf"
cf.read(conf_path)

import redis
rd_host = cf.get("redis","host")
rd_port = cf.get("redis","port")
rd_pwd = cf.get("redis","pwd")
rd = redis.Redis(host=rd_host,port=rd_port,db=0,password=rd_pwd)
pipe = rd.pipeline()

import psycopg2
db_host = cf.get("database","host")
db_dbname = cf.get("database","dbname")
db_user = cf.get("database","user")
db_pwd = cf.get("database","pwd")
conn = psycopg2.connect(host=db_host,dbname=db_dbname,user=db_user,password=db_pwd)
cur = conn.cursor()

from pykafka import KafkaClient
ka_hosts = cf.get("kafka","kafka_hosts")
zp_hosts = cf.get("kafka","zookeeper_hosts")
ka_topics = cf.get("kafka","topics")
ka_group = cf.get("kafka","group")
client = KafkaClient(hosts=ka_hosts)
topic = client.topics[ka_topics]
consumer = topic.get_simple_consumer(consumer_group=ka_group)

