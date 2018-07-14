from pyspark.sql import SparkSession,Row,functions
from pyspark import SparkContext
from ast import literal_eval
from pyspark.ml.linalg import Vector,Vectors
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer,HashingTF, Tokenizer,IDF
from pyspark.ml.classification import LogisticRegression,LogisticRegressionModel,BinaryLogisticRegressionSummary, LogisticRegression

spark = SparkSession.builder.getOrCreate()
cleanedData=spark.sparkContext.textFile("tokenizedData").map(lambda r:literal_eval(str(r)))
df=cleanedData.toDF(["label","words"])
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featuredData=hashingTF.transform(df)
idf=IDF(inputCol="rawFeatures",outputCol="features")
idfModel=idf.fit(featuredData)
rescaledData=idfModel.transform(featuredData)
rescaledData.select("label","features").show()
inputData=rescaledData

#index label and feature
labelIndexer = StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(inputData)
featureIndexer = VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(inputData)
#split the inputData into trainingData and testData
trainingData, testData = inputData.randomSplit([0.7,0.3])
#lr will train the model
lr = LogisticRegression().setLabelCol("indexedLabel")\
.setFeaturesCol("indexedFeatures").setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
#convert predicted label by lableindexer
labelConverter = IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
#lr pipeline set
lrPipeline =  Pipeline().setStages([labelIndexer, featureIndexer, lr, labelConverter])
#model trained
lrPipelineModel = lrPipeline.fit(trainingData)
#predicted data
lrPredictions = lrPipelineModel.transform(testData)

preRel = lrPredictions.select("predictedLabel", "label", "features", "probability").collect()
for item in preRel:
    print str(item['label'])+','+str(item['features'])+'-->prob='+str(item['probability'])+',predictedLabel'+str(item['predictedLabel'])
evaluator = MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
lrAccuracy = evaluator.evaluate(lrPredictions)
print("Test Error = " + str(1.0 - lrAccuracy))
