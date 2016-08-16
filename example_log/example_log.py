#from pyspark import SparkContext

#sc = SparkContext("local[2]","example_log app")
#log4j = sc._jvm.org.apache.log4j
#log = log4j.LogManager.getLogger(__name__)
#log.debug("this is a debug log")
#log.info("this is a info log")
#log.error("this is a error log")

import pudb;
pu.db
import logging
#logging.basicConfig(filename='logger.log',level=logging.INFO)
logging.basicConfig(level=logging.DEBUG)
plog = logging.getLogger(__name__)

plog.debug("this is a debug plog")
plog.info("this is a info plog")
plog.error("this is a error plog")


