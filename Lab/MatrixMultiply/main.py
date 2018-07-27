from pyspark import SparkContext
m=0
n=0
p=0
def mapper(lst):
    #lst=['A'(or 'B'),x,y,value]
    x=lst[1]
    y=lst[2]
    value=lst[3]
    if lst[0]=='A':
        return [((int(x),i,int(y)),float(value)) for i in range(p)]
    if lst[0]=='B':
        return [((i,int(y),int(x)),float(value))for i in range(m)]

sc=SparkContext()
rdd=sc.textFile("input.txt").map(lambda x:x.split(','))
rdd=rdd.flatMap(mapper)
print rdd.collect()
rdd=rdd.reduceByKey(lambda a,b:a*b)
rdd=rdd.map(lambda (x,y):((x[0],x[1]),y))
rdd=rdd.reduceByKey(lambda a,b:a+b)
print rdd.collect()
