from  pyspark.mllib.recommendation import ALS,Rating
from pyspark import  SparkContext
from pyspark.sql import SQLContext,Row
import os

os.environ['SPARK_CLASSPATH'] = "/home/leo/spark/lib/postgresql-9.3-1103.jdbc41.jar"
sc = SparkContext("local[2]","first spark app")
sqlContext = SQLContext(sc)
url = "jdbc:postgresql://localhost/sparktest?user=leo&password=123123"
data = sqlContext.load(source="jdbc",url=url,dbtable="public.user_rating")
print data.first()
ratings = data.map(lambda x:Rating(int(x[0]),int(x[1]),float(x[2])))
#print ratings.first()
model = ALS.train(ratings,50)
#features = model.userFeatures()
#print features.take(2)
predict = model.predict(2,2)
print predict
top = model.recommendProducts(2,10)
print top



