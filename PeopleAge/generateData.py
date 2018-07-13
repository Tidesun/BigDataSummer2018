from random import *
with open("input.txt","w") as f:
    for i in range(100000):
        if random()<0.5:
            x="F"
        else:
            x="M"
        f.write(str(i)+" "+x+" "+str(randrange(140,210))+'\n')
