import os
from pyspark import SparkContext
from job3 import utils as utils
import time

from itertools import islice


os.system("rm -r outputJob3")

start = time.time()


sc = SparkContext(appName="job3")


#skip first line
#(ticker) -> (close, anno, data)
second_table = sc.textFile("../input/SecondTable")\
            .mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it) \
            .map(lambda line: (utils.getString(line, 0), [utils.getDouble(line, 2), utils.getAnno(utils.getString(line, 7)),
                                                            utils.getString(line, 7)])) \
            .filter(lambda x: 2016 <= x[1][1] <= 2018)

#(ticker) -> (nome, settore)
first_table = sc.textFile("../input/FirstTable") \
                .mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it) \
                .map(lambda line: (utils.getString(line, 0), utils.sistemaValori(line))) \
                .join(second_table)

#(ticker, nome, settore, anno) -> (close, data)
#reduce: calcolo minimo e massimo dell'anno
#map: calcolo la differenza percentuale
#reduce: sommo le differenze percentuali
#map&reduce: (nome, settore) -> [(2016, diff), (2017, diff), (2018, diff)]
#map: (2016, 2017, 2018) -> (nome, settore)
one = first_table.map(lambda x: ((x[0], x[1][0][0], x[1][0][1], x[1][1][1]), [(x[1][1][2], x[1][1][0]), (x[1][1][2], x[1][1][0])])) \
                    .reduceByKey(lambda x, y: [utils.mindataclose(x[0], y[0]), utils.maxdataclose(x[1], y[1])]) \
                    .map(lambda x: ((x[0][1], x[0][2], x[0][3]), (x[1][1][1]/x[1][0][1]*100)-100)) \
                    .reduceByKey(lambda x, y: x + y) \
                    .map(lambda x: ((x[0][0], x[0][1]), [(x[0][2], x[1]), (x[0][2], x[1]), (x[0][2], x[1])])) \
                    .reduceByKey(lambda x, y: [utils.el2016(x[0], y[0]), utils.el2017(x[1], y[1]), utils.el2018(x[2], y[2])]) \
                    .map(lambda x: ((x[1][0], x[1][1], x[1][2]), x[0]))

#join con se stesso + rimuovo colonne che hanno settore e nome uguale
result = one.join(one) \
        .filter(lambda line: line[1][0][1] != line[1][1][1] and line[1][0][0] != line[1][1][0])

result.coalesce(1).saveAsTextFile("../outputJob3")
end = time.time()

print("execution time is: "+ str((end-start)/60) + " min")