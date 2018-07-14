from pyspark import SparkContext
sc=SparkContext()
input=sc.textFile("input.txt").map(lambda x:filter(lambda item:item!='',x.split(" "))).map(lambda x:(x[0],x[1]))
RevInput=input.map(lambda x:(x[1],x[0]))
res=RevInput.join(input).values().collect()
for item in res:
    print item[0],item[1]
