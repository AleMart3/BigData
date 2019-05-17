#ticker anno -> close low, high volume

# media chiusura
# minimo low
# massimo high
# somma volume + conteggio

import os
from pyspark import SparkContext
import utils as utils


from itertools import islice


os.system("rm -r output")


sc = SparkContext(appName="job1")

#(ticker, anno) -> (media close, min, max, somma volumi, cont)
#skip first line
text_file = sc.textFile("input/SecondTable")\
        .mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

counts = text_file.map(lambda line: ((utils.getString(line, 0), utils.getAnno(utils.getString(line,7))),
                                        (utils.getDouble(line, 2), utils.getDouble(line, 4),
                                         utils.getDouble(line, 5), utils.getDouble(line, 6), 1)))\
        .filter(lambda x: 1997 < x[0][1] < 2019) \
        .reduceByKey(lambda x, y: (x[0]+y[0], min(x[1], y[1]), max(x[2], y[2]), x[3]+y[3], x[4] + y[4])) \
        .map(lambda x: (x[0][0], utils.flattuple((x[0][1], x[1])))) \
        .reduceByKey(lambda x, y: [utils.minannoclose(x[0],y[0]), utils.maxannoclose(x[1], y[1]),
                                  min(x[2],y[2]), max(x[3],y[3]), x[4]+y[4], x[5]+y[5]]) \
        .map(lambda x: (x[0], utils.result(x[1]))) \
        .sortBy(lambda x: x[1][0], ascending=False).take(10)

sc.parallelize(counts).coalesce(1).saveAsTextFile("output/output.txt")

# (ticker) -> ((annominimo, close), (annomassimo, close), minimo low, max high, somma volumi, cont)
# (ticker) -> (differenza, incremento, minimo, massimo, media volumi)
#differenza close, incremento percentuale, media volumi











