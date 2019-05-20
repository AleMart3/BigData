from datetime import datetime


def getString(s, num):
    return s.split(",")[num]


def getDouble(s, num):
    return float(s.split(",")[num])


def flattuple(tuple):
    list = [(tuple[0], tuple[1][0]/tuple[1][4]), (tuple[0], tuple[1][0]/tuple[1][4])]  #tupla duplicata per calcolare successivamente anno minimo e massimo
    for i in range (1, 5):                                                              #mi serve il valore duplicato
        list.append(tuple[1][i])
    return list

def maxannoclose(tuple1, tuple2):
    if tuple1[0] < tuple2[0]:
        return tuple2
    return tuple1

def minannoclose(tuple1, tuple2):
    if tuple1[0] > tuple2[0]:
        return tuple2
    return tuple1

def getAnno(data):
    return (datetime.strptime(data, '%Y-%m-%d').year)


def getAnnoMinimo(counts):
    annominimo = 3000
    for el in counts:
        if el[0] < annominimo:
            annominimo = el[1]
    return annominimo


def getAnnoMassimo(counts):
    annomassimo = 0
    for el in counts:
        if el[0] > annomassimo:
            annomassimo = el[1]
    return annomassimo


def result(list):
    # restituisce differenza,       incremento percentuale,         minimo    massimo, media volumi
    return [list[1][1]-list[0][1], (list[1][1]/list[0][1]*100)-100, list[2], list[3], list[4]/list[5]]

#reduceByKey ticker -> ((annominimo, close), (annomassimo, close), minimo low, max high, somma volumi, cont)


