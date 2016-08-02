#from pyspark import SparkContext

#sc = SparkContext("local[2]","example_log app")
#log4j = sc._jvm.org.apache.log4j
#log = log4j.LogManager.getLogger(__name__)
#log.debug("this is a debug log")
#log.info("this is a info log")
#log.error("this is a error log")


import logging
import logging.config
logging.config.fileConfig("log.conf")
log = logging.getLogger("example_log")
log.debug("this is a debug log")
log.info("this is a info log")
log.error("this is a error log")
