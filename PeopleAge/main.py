from pyspark import SparkContext

sc=SparkContext()
lines=sc.textFile("input.txt")
maleinfo=lines.filter(lambda x:"M" in x).map(lambda x:(int(x.split(" ")[0]),int(x.split(" ")[2])))
femaleinfo=lines.filter(lambda x:"F" in x).map(lambda x:(int(x.split(" ")[0]),int(x.split(" ")[2])))
malecounts=maleinfo.count()
femalecounts=femaleinfo.count()

print maleinfo.takeOrdered(1,key=lambda(w,c):-c)
print maleinfo.takeOrdered(1,key=lambda(w,c):c)
print femaleinfo.takeOrdered(1,key=lambda(w,c):-c)
print femaleinfo.takeOrdered(1,key=lambda(w,c):c)
