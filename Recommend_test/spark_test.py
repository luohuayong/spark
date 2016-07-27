from pyspark import SparkContext,SparkConf
from pyspark.mllib.recommendation import ALS,Rating
conf = SparkConf()
conf.set('spark.app.name','spark_test app')
sc = SparkContext(appName='spark_test app')
log4j = sc._jvm.org.apache.log4j
log = log4j.LogManager.getLogger(__name__)

rdd_data = sc.parallelize(([1,1,1.0],[1,2,2.0],[2,1,1.0]))
ratings = rdd_data.map(lambda x:Rating(int(x[0]),int(x[1]),float(x[2])))
model = ALS.train(ratings,50)
print model.predict(2,2)
