import os, sys, string, time, operator

ProjDir = os.path.abspath(os.path.dirname(__file__)) + "/"  #define the proj dir

inputFile = ""
outputWordNumber = 0
wordCount = {}
if __name__ == "__main__":
    if len(sys.argv) != 3:
        #argument num should be 3
        print ("Wrong input parameter.")
        print ("Usage: python PythonWordCount.py [input file name] [number of output words]")
        exit(0)
inputfile=sys.argv[1]
numoutwords=int(sys.argv[2])
start_time=time.time()
f=open(ProjDir+inputfile)
lines=f.readlines()
f.close()
for line in lines:
    line=line.translate(None,string.punctuation) #delete punctuation by translate
    wordArr=line.split()    #split line to wordarr
    for word in wordArr:
        word=word.lower()
        if word in wordCount:
            wordCount[word]+=1
        else:
            wordCount[word]=1
wordCount=sorted(wordCount.items(), key=operator.itemgetter(1),reverse=True)
i=0
for key,item in wordCount:
    print key,' ',item
    i+=1
    if (i>9):break
print time.time()-start_time
