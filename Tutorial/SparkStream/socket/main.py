from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc=SparkContext("local[2]", "NetworkWordCount") # 2 working threads 1 for run the receiver other for process
ssc=StreamingContext(sc,1)  #batch interval:1 second
lines=ssc.socketTextStream("localhost",9999) #create a Dstream receving data from localhost:9999
words=lines.map(lambda x:x.decode()).flatMap(lambda line:line.split(" ")) #split each line into words list
pair=words.map(lambda word:(word,1)) #count each word
wordCounts=pair.reduceByKey(lambda a,b:a+b) #add up the count for each word
wordCounts.pprint()

ssc.start()
ssc.awaitTermination()
