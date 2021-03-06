import os
from pyspark import SparkContext, SparkConf
from job2 import utils as utils
import time



from itertools import islice


os.system("rm -r ../outputJob2")

start= time.time()

sp = SparkConf().setMaster("local[8]")
sc = SparkContext(appName="job2", conf=sp)


#(ticker) -> (close,volume,anno,data,1)
#skip first line
second_table = sc.textFile("../input/historical_stock_prices.csv")\
            .mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it) \
            .map(lambda line: (utils.getString(line, 0), [utils.getDouble(line, 2), utils.getDouble(line, 6),
                                                            utils.getAnno(utils.getString(line, 7)),
                                                            utils.getString(line, 7), 1])) \
            .filter(lambda x: 2004 <= x[1][2] <= 2018)

#(ticker) -> (settore)
first_table = sc.textFile("../input/historical_stocks.csv") \
                .mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it) \
                .map(lambda line: (utils.getString(line, 0), utils.sistemaValori(line))) \
                .join(second_table)

#PER SEGUIRE GLI INDICI UTILIZZATI NEL CODICE, BASTA FAR RIFERIMENTO AI COMMENTI DI SEGUITO
#join (ticker, (settore , [close, volume, anno, data, 1] ) )
#map (settore, anno, ticker) -> ((data, close), (data, close), volume, cont, close), abbiamo replicato il valore di close per fare la somma
#reduce (settore, anno, ticker) -> (min (data, close), max (data, close), volume, cont, sommaclose)
#map (settore, anno, ticker) -> (incrementopercentuale, sommavol, cont)
#map (settore, anno) -> (valori)
#reduce (sommaincrementi, sommavol, sommacont)
result = first_table.map(lambda x: ((x[0], x[1][0], x[1][1][2]), [(x[1][1][3], x[1][1][0]), (x[1][1][3], x[1][1][0]),
                                                                   x[1][1][1], x[1][1][4], x[1][1][0]])) \
                    .reduceByKey(lambda x, y: [utils.mindataclose(x[0],y[0]), utils.maxdataclose(x[1], y[1]), x[2]+y[2], x[3]+y[3], x[4]+y[4]]) \
                    .map(lambda x: (x[0], ((x[1][1][1]/x[1][0][1]*100)-100, x[1][2], x[1][3], x[1][4]))) \
                    .map(lambda x: ((x[0][1], x[0][2]), x[1])) \
                    .reduceByKey(lambda x, y: [x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3]])\
                    .map(lambda x: (x[0], [x[1][0], x[1][1], x[1][3]/x[1][2]]))

result.coalesce(1).saveAsTextFile("../outputJob2")
end = time.time()

print("execution time is: "+ str((end-start)/60) + " min")