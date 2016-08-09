#from pyspark import SparkContext

#sc = SparkContext("local[2]","example_log app")
#log4j = sc._jvm.org.apache.log4j
#log = log4j.LogManager.getLogger(__name__)
#log.debug("this is a debug log")
#log.info("this is a info log")
#log.error("this is a error log")

import json
import logging
import logging.config
log_json_path = "log.json"
with open(log_json_path,'rt') as f:
    config = json.load(f)
logging.config.dictConfig(config)
#logging.config.fileConfig("log.conf")
log = logging.getLogger(__name__)
log.debug("this is a debug log")
log.info("this is a info log")
log.error("this is a error log")
