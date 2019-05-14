from datetime import datetime



def getString(s, num):
    return s.split(",")[num]

def getDouble(s, num):
    return float(s.split(",")[num])




def getAnno(data):
    return (datetime.strptime(data, '%Y-%m-%d').year)

