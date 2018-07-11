import sys,os
from pyspark import SparkContext
sc=SparkContext()
fileName = os.path.abspath(os.path.dirname(__file__))
crashData = sc.textFile(fileName + "/crash.txt").map(lambda r:str(r)).map(lambda line: line.split(" "))
crashDataRDD = crashData.map(lambda t:(0,t))
nearCrashData = sc.textFile(fileName + "/nearCrash.txt").map(lambda r:str(r)).map(lambda line: line.split(" "))
nearCrashDataRDD = nearCrashData.map(lambda t:(1,t))
otherTypeData=sc.textFile(fileName + "/otherType.txt").map(lambda r:str(r)).map(lambda line: line.split(" "))
otherTypeDataRDD=otherTypeData.map(lambda t:(2,t))
def tokenize(text):
    lowercase=[word.lower() for word in text]
    return text
primaryRDD=crashDataRDD+nearCrashDataRDD+otherTypeDataRDD
tokenizedRDD=primaryRDD.map(lambda(label,text):(label,tokenize(text)))
tokenizedRDD.saveAsTextFile("tokenizedData")
