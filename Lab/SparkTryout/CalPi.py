import sys
from pyspark import SparkContext
from random import random
def sample(p):
    x, y = random(), random()
    return 1 if x*x + y*y < 1 else 0
sc = SparkContext()
count = sc.parallelize(range(0, 10000)).map(sample).reduce(lambda a, b: a + b)
print ("Pi is roughly %f" % (4.0 * count / 10000))
