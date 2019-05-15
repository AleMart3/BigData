#ticker anno -> close low, high volume

# media chiusura
# minimo low
# massimo high
# somma volume + conteggio
import itertools
import os
from pyspark import SparkContext
import utils as utils


from itertools import islice


#os.system("rm -r output")


sc = SparkContext(appName="job1")

#(ticker, anno) -> (media close, min, max, somma volumi, cont)
#skip first line
text_file = sc.textFile("input/SecondTable")\
        .mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

counts = text_file.map(lambda line: ((utils.getString(line,0),utils.getAnno(utils.getString(line,7))),
                                        (utils.getDouble(line,2),utils.getDouble(line,4),utils.getDouble(line,5),utils.getDouble(line,6)))  )\
        .filter(lambda x : 1997 < x[0][1] < 2019) \
        .reduceByKey(lambda x , y: ((x[0]+y[0])/(1+1), min(x[1], y[1]), max(x[2],y[2]), (x[3]+y[3]), (1 + 1))) \
        .map(lambda x: (x[0][0], utils.flattuple((x[0][1], x[1]))))\
        .reduceByKey(lambda x, y: (min(x[0],y[0])))
      #  .reduceByKey(lambda x, y: (min(x[0],y[0]), max(x[0],y[0]), min(x[2], y[2]), max(x[3],y[3]), sum(x[4],y[4]), sum(x[5],y[5]))).collect()

print (counts.collect())











