from datetime import datetime


def getString(s, num):
    return s.split(",")[num]


def getDouble(s, num):
    return float(s.split(",")[num])


def flattuple(tuple):
    list = [(tuple[0], tuple[1][0])]
    for i in range(1, 5):
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
    annoMin = getAnnoMinimo(list)
    annoMax = getAnnoMassimo(list)

    for el in list:
        if el[0] == annoMin:
            closeMin = el[2]
        if el[0] == annoMax:
            closeMax = el[2]

    return closeMax - closeMin, (closeMax/closeMin*100)-100



