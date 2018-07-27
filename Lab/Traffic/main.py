import sys,os,string
from pyspark import SparkContext
from nltk.stem import WordNetLemmatizer
from nltk.stem import *

sc=SparkContext()

fileName = os.path.abspath(os.path.dirname(__file__))

crashData = sc.textFile(fileName + "/crash.txt").map(lambda r:str(r)).map(lambda line: line.split(" ")).filter(lambda x:x!=' ')
crashDataRDD = crashData.map(lambda t:(0,t))
nearCrashData = sc.textFile(fileName + "/nearCrash.txt").map(lambda r:str(r)).map(lambda line: line.split(" ")).filter(lambda x:x!=' ')
nearCrashDataRDD = nearCrashData.map(lambda t:(1,t))
otherTypeData=sc.textFile(fileName + "/otherType.txt").map(lambda r:str(r)).map(lambda line: line.split(" ")).filter(lambda x:x!=' ')
otherTypeDataRDD=otherTypeData.map(lambda t:(2,t))

stopWordsSet=set(sc.textFile("stop-word-list.txt").collect())

lemmatizer = WordNetLemmatizer()

def tokenize(text):
    lowercase=[word.lower() for word in text]

    depunctuated=[word.translate(None,string.punctuation) for word in lowercase]

    stopped=[word for word in depunctuated if word not in stopWordsSet]

    lemmatized = [lemmatizer.lemmatize(word) for word in stopped]
    return lemmatized

primaryRDD=(crashDataRDD+nearCrashDataRDD+otherTypeDataRDD)
tokenizedRDD=primaryRDD.map(lambda(label,text):(label,tokenize(text)))

tokenizedRDD.saveAsTextFile("tokenizedData")
