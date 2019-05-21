from datetime import datetime


def getString(s, num):
    return s.split(",")[num]


def getDouble(s, num):
    return float(s.split(",")[num])


def sistemaValori(line):
    list = line.split(",")

    sector = list[len(list) - 2]
    if "\"" in sector:
        sector = list[len(list) - 3]

    return sector


def maxdataclose(tuple1, tuple2):
    dataTupla1 = getData(tuple1[0])
    dataTupla2 = getData(tuple2[0])
    if dataTupla1 < dataTupla2:
        return tuple2
    return tuple1


def mindataclose(tuple1, tuple2):
    dataTupla1 = getData(tuple1[0])
    dataTupla2 = getData(tuple2[0])
    if dataTupla1 > dataTupla2:
        return tuple2
    return tuple1

def getAnno(data):
    return datetime.strptime(data, '%Y-%m-%d').year

def getData(data):
    return datetime.strptime(data, '%Y-%m-%d').date()