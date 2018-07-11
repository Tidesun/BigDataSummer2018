import sys,string,re
from pyspark import SparkContext
sc=SparkContext()
def removePunctuation(text):
    return re.sub(r'[^a-z0-9\s]', '', text.lower().strip())
def wordCount(rdd):
    pairs = rdd.map(lambda s: (s, 1))
    counts = pairs.reduceByKey(lambda a, b: a + b)
    return counts
#1.remove punctuation
#2.split the text return a list of numoutwords
#3.filter out words with only space like " "
#4.count the words
rmedPuncRdd=(sc.textFile("input.txt",8)).map(removePunctuation)
splitRdd=rmedPuncRdd.flatMap(lambda x:x.split(' '))
filterRdd=splitRdd.filter(lambda x:x!='')
#filterRdd.saveAsTextFile("output")
top15WordsAndCounts = wordCount(filterRdd).takeOrdered(20, key=lambda (w,c): -c)
print '\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), top15WordsAndCounts))
