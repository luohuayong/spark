from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import time
sc = SparkContext(appName="engine")
ssc = StreamingContext(sc,5)

serverIP = "127.0.0.1"
serverPort = 9999
ds_lines = ssc.socketTextStream(serverIP,serverPort)
ds_rating = ds_lines.map(lambda line:(line.split(",")))
ds_rating.pprint()

ssc.start()
time.sleep(120)
ssc.stop(stopSparkContext=True,stopGraceFully=True)
#ssc.awaitTermination()
