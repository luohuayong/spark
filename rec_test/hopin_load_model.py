from pyspark import SparkContext
from pyspark.mllib.recommendation import MatrixFactorizationModel
model_path = "hopin_model"
sc = SparkContext(appName = "load_model")
model = MatrixFactorizationModel.load(sc,model_path)
top = model.recommendProducts(61,30)
print top
