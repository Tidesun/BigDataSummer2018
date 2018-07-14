from pyspark import SparkContext

sc=SparkContext()
lines=sc.textFile("input.txt")
countrdd=lines.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1))
countrdd=countrdd.reduceByKey(lambda a,b:a+b)
print countrdd.count()
countidrdd=lines.map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)
print countidrdd.count()
countfollowerrdd=lines.map(lambda x:(x.split(" ")[len(x.split(" "))-1],x.split(" ")[0])).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)
print countfollowerrdd.count()
