from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression

spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext
# col=["normal","hypervent"]
# df=sc.parallelize([(56,87),(56,91),(65,85),(65,91),(50,75),(25,28),(87,122),(44,66),(35,58)],2).toDF(col)
# r1=df.corr("normal","hypervent")
# print "Pearson correlation matrix:\n"+str(r1)

training = spark.createDataFrame([
    (1.0, Vectors.dense([0.0, 1.1, 0.1])),
    (0.0, Vectors.dense([2.0, 1.0, -1.0])),
    (0.0, Vectors.dense([2.0, 1.3, 1.0])),
    (1.0, Vectors.dense([0.0, 1.2, -0.5]))], ["label", "features"])
lr=LogisticRegression(maxIter=10,regParam=0.01)
model1=lr.fit(training)
print "model1 using parameters:"
print model1.extractParamMap()

test = spark.createDataFrame([
    (1.0, Vectors.dense([-1.0, 1.5, 1.3])),
    (0.0, Vectors.dense([3.0, 2.0, -0.1])),
    (1.0, Vectors.dense([0.0, 2.2, -1.5]))], ["label", "features"])

prediction=model1.transform(test)
result=prediction.collect()
print result
