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
pipeline =  Pipeline().setStages([labelIndexer, featureIndexer, lr, labelConverter])
#model trained
paramGrid = ParamGridBuilder()\
            .addGrid(lr.regParam, [0.1, 0.01])\
            .addGrid(lr.elasticNetParam,[0.1,0.5,0.9])\
            .build()
crossval=CrossValidator(estimator=pipeline,\
                          estimatorParamMaps=paramGrid,\
                          evaluator=MulticlassClassificationEvaluator(),\
                          numFolds=3)
cvModel=crossval.fit(training)
cvModel.bestModel.stages[2].summary
