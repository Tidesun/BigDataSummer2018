from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os

ProjDir = os.path.abspath(os.path.dirname(__file__)) + "/"
print(ProjDir)
sc=SparkContext("local[2]", "WordCount") # 2 working threads 1 for run the receiver other for process
rdd=sc.textFile("hdfs:/"+ProjDir+"input.txt")
rdd.collect()
# ssc=StreamingContext(sc,1)  #batch interval:1 second
# lines=ssc.textFileStream("hdfs:/"+ProjDir+"input.txt")
# words=lines.flatMap(lambda line:line.split(" ")) #split each line into words list
# pair=words.map(lambda word:(word,1)) #count each word
# wordCounts=pair.reduceByKey(lambda a,b:a+b) #add up the count for each word
# wordCounts.pprint()
#
# ssc.start()
# ssc.awaitTermination()
