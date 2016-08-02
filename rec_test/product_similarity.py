from pyspark import SparkContext
from pyspark.mllib.recommendation import MatrixFactorizationModel
import numpy as np
def consineSimilarity(x,y):
    return np.dot(x,y)/(np.linalg.norm(x)*np.linalg.norm(y))
model_path = "hopin_model"
sc = SparkContext(appName = "load_model")
model = MatrixFactorizationModel.load(sc,model_path)
top = model.recommendProducts(61,30)
print top
