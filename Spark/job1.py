#ticker anno -> close low, high volume

# media chiusura
# minimo low
# massimo high
# somma volume + conteggio

import os
from pyspark import SparkContext
import utils as utils


from itertools import islice


#os.system("rm -r output")


sc = SparkContext(appName="job1")


#skip first line
text_file = sc.textFile("input/SecondTable")\
        .mapPartitionsWithIndex( lambda idx, it: islice(it, 1, None) if idx == 0 else it)

counts = text_file.map(lambda line: (   (utils.getString(line,0),utils.getAnno(utils.getString(line,7))) ,
                                        (utils.getDouble(line,2),utils.getDouble(line,4),utils.getDouble(line,5),utils.getDouble(line,6)))  )\
        .reduceByKey(lambda x, y: ((x[0]+y[0])/(1+1) ,  min(x[1] ,y[1]) , max(x[2],y[2])  , (x[3]+y[3] )  , (1+1 )  )).collect()
                            #ticker anno -> mediaChiusura, minlow, maxhigh, sommavolumi, conteggio



print(counts)









