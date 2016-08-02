from  pyspark.mllib.recommendation import ALS,Rating
from pyspark import  SparkContext

sc = SparkContext("local[2]","first spark app")
rawdata = sc.textFile("./ml-100k/u.data")
#print rawdata.first()
data = rawdata.map(lambda x:x.split('\t'))
#print data.take(5)
ratings = data.map(lambda x:Rating(int(x[0]),int(x[1]),float(x[2])))
#print ratings.first()
model = ALS.train(ratings,50)
features = model.userFeatures()
#print features.take(2)
predict = model.predict(789,123)
print predict
top = model.recommendProducts(789,10)
print top


