from datetime import datetime

def el2016(tuple1, tuple2):
    if tuple2[0] == 2016:
        return tuple2
    return tuple1

def el2017(tuple1, tuple2):
    if tuple2[0] == 2017:
        return tuple2
    return tuple1

def el2018(tuple1, tuple2):
    if tuple2[0] == 2018:
        return tuple2
    return tuple1

def sistemaValori(line):
    list = line.split(",")
    campi = []

    if len(list) == 7 or (len(list) == 6 and "\"" in list[2]):
        nome = list[2] + " " + list[3]
        campi.append(nome[1:-1])
        campi.append(list[4])
    else:
        campi.append(list[2])
        campi.append(list[3])

    print (campi)
    return campi

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


def getString(s, num):
    return s.split(",")[num]


def getDouble(s, num):
    return float(s.split(",")[num])

def getAnno(data):
    return datetime.strptime(data, '%Y-%m-%d').year

def getData(data):
    return datetime.strptime(data, '%Y-%m-%d').date()