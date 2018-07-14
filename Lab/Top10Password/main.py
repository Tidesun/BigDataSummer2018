from pyspark import SparkContext
sc=SparkContext()
password=sc.textFile("input.txt").map(lambda x:x.split("#")).map(lambda x:(x[1],1))
pwcount=password.reduceByKey(lambda a,b:a+b)
top20pw=pwcount.takeOrdered(20,key=lambda(w,c):-c)
print '\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), top20pw))
