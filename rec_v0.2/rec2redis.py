import logging
import logging.config
import json
with open("log.json",'rt') as f:
    config = json.load(f)
logging.config.dictConfig(config)
log = logging.getLogger(__name__)

import ConfigParser
cf = ConfigParser.ConfigParser()
conf_path = "/mnt/conf/engine.conf"
cf.read(conf_path)

import redis
#rd_host = cf.get("redis","host")
#rd_port = cf.get("redis","port")
rd = redis.Redis(host='127.0.0.1',port=6379,db=0)

import psycopg2
db_host = cf.get("database","host")
db_dbname = cf.get("database","dbname")
db_user = cf.get("database","user")
db_pwd = cf.get("database","pwd")
conn = psycopg2.connect(host=db_host,dbname=db_dbname,user=db_user,password=db_pwd)

log.info("start write redis ...")
cur = conn.cursor()
sql_recommend = "select uid,pid,rating from user_recommend where uid=61"
cur.execute(sql_recommend)
rows = cur.fetchall()
json_rec = json.dumps(rows)
rd.set("user_recommend:uid:61",json_rec)
conn.commit()
cur.close()
conn.close()
log.info("write redis complete")


