import sys,os,string
from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF,Tokenizer
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Word Count").getOrCreate()

training = spark.createDataFrame([
    (0, "a b c d e spark", 1.0),
    (1, "b d", 0.0),
    (2, "spark f g h", 1.0),
    (3, "hadoop mapreduce", 0.0)
], ["id", "text", "label"])
tokenizer=Tokenizer(inputCol="text",outputCol="words")#tokenizer to tokenize the data
hashingTF=HashingTF(inputCol=tokenizer.getOutputCol(),outputCol="features")#feature Extract using hashingTF
lr=LogisticRegression(maxIter=10,regParam=0.001)#lr to train the model
pipeline=Pipeline(stages=[tokenizer,hashingTF,lr])#pipeline

model=pipeline.fit(training)#train the model
test = spark.createDataFrame([
    (4, "spark i j k"),
    (5, "l m n"),
    (6, "spark hadoop spark"),
    (7, "apache hadoop")
], ["id", "text"])

prediction = model.transform(test)#use model to predict the result
selected = prediction.select("id", "text", "probability", "prediction")
for row in selected.collect():
    rid, text, prob, prediction = row
    print("(%d, %s) --> prob=%s, prediction=%f" % (rid, text, str(prob), prediction))
