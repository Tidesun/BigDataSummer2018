#!/usr/bin/env python
# encoding=utf-8
import sys,os,jieba
from pyspark import SparkContext
sc=SparkContext()
fileName = os.path.abspath(os.path.dirname(__file__))
wordList=[]
with open("input.txt","r") as f:
    lines=f.readlines()
    for line in lines:
        line=line.decode('gb2312')
        wordList+= jieba.lcut(line)
rdd=sc.parallelize(wordList).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)
print rdd.collect()
