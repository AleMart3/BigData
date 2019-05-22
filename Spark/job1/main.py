#ticker anno -> close low, high volume

# media chiusura
# minimo low
# massimo high
# somma volume + conteggio

import os
from pyspark import SparkContext, SparkConf
from job1 import utils as utils
import time


from itertools import islice


os.system("rm -r outputJob1")

start= time.time()

sp = SparkConf().setMaster("local[8]")
sc = SparkContext(appName="job1", conf=sp)


#skip first line
text_file = sc.textFile("input/historical_stock_prices.csv")\
        .mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

counts = text_file.map(lambda line: ((utils.getString(line, 0), utils.getAnno(utils.getString(line, 7))),
                                        (utils.getDouble(line, 2), utils.getDouble(line, 4),
                                         utils.getDouble(line, 5), utils.getDouble(line, 6), 1)))\
        .filter(lambda x: 1998 <= x[0][1] <= 2018)\
        .reduceByKey(lambda x, y: (x[0]+y[0], min(x[1], y[1]), max(x[2], y[2]), x[3]+y[3], x[4] + y[4])) \
        .map(lambda x: (x[0][0], utils.flattuple((x[0][1], x[1])))) \
        .reduceByKey(lambda x, y: [utils.minannoclose(x[0], y[0]), utils.maxannoclose(x[1], y[1]),
                                   min(x[2],y[2]), max(x[3],y[3]), x[4] + y[4], x[5] + y[5]]) \
        .map(lambda x: (x[0], utils.result(x[1]))) \
        .sortBy(lambda x: x[1][0], ascending=False).take(10)

sc.parallelize(counts).coalesce(1).saveAsTextFile("outputJob1")


end= time.time()

print("execution time is: "+ str((end-start)/60) + " min")


#map    (ticker,anno) -> (close,low,high,volume,1)
#filter  filtra anni compresi tra 1998 e 2018
#reduceByKey  (ticker,anno)-> (somma_close, min, max, somma_volumi, cont)         aggrega in base alla chiave specificata in map
#map    ticker -> (anno,close), (anno,close), min, max, media, cont           viene duplicata la tupla (anno,close) per calcolare nella reduce, anno minimo e max
#reduceByKey ticker -> ((annominimo, close), (annomassimo, close), minimo low, max high, somma volumi, cont)
#map   ticker -> (differenza, incremento, minimo, massimo, media volumi)
#sort ordina in base alla differenza e prende le prime 10












