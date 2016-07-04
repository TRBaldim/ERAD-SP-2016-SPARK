'''
Created on 02/07/2016

@author: thiago
'''

from pyspark import SparkContext
from datetime import datetime
from graficos import plotData


if __name__ == '__main__':
    
    byCountry = 'GlobalLandTemperatures/GlobalLandTemperaturesByCountry.csv'
    byGlobal = 'GlobalLandTemperatures/GlobalTemperatures.csv'
    
    sc = SparkContext(master='local', appName='ERAD-SP')
    
    ClimateRdd = sc.textFile(byCountry, use_unicode=False)
    
    ClimateRdd = ClimateRdd.map(lambda line: line.split(','))

    header = ClimateRdd.first()
    
    ClimateRdd = ClimateRdd.filter(lambda line: line != header)
        
    CityByDate =  ClimateRdd.map(lambda x: (x[0], (float(x[1] if x[1] != '' else 0.0), 1))) \
                            .reduceByKey(lambda val, acc: (val[0] + acc[0], val[1] + acc[1])) \
                            .map(lambda x: (x[0], x[1][0] / x[1][1])) \
                            .sortBy(lambda x: x[0])
                                
    convertedeDate = CityByDate.map(lambda x: (datetime.strptime(x[0], '%Y-%m-%d'), x[1]))
        
    plotData(convertedeDate.collect())
    
    globalRdd = sc.textFile(byGlobal, use_unicode=False)
        
    header = globalRdd.first()
    
    globalRdd = globalRdd.filter(lambda x: x != header) \
                         .map(lambda x: x.split(',')) \
                         .map(lambda x: (x[0], float(x[1]) if x[1] != '' else 0.0)) \
                         .sortBy(lambda x: x[0])
    
    convertedeDate = globalRdd.map(lambda x: (datetime.strptime(x[0], '%Y-%m-%d'), x[1]))
    
    plotData(convertedeDate.collect())
    
    globalVsCities = globalRdd.join(CityByDate) \
                              .sortBy(lambda x: x[0])
    def checkTemp(elem):
        
        modulo = lambda x: x if x > 0 else -x
        temp = elem[1]
        
        if modulo(temp[0]-temp[1]) > 5:
            return elem
        else:
            return None
    
    tempDeviations = globalVsCities.map(checkTemp)
    
    tempDeviations = tempDeviations.filter(lambda x: x != None)
    
    '''
    Agora como saber qual a temperatura que tem maior diferenca?
    Vamos fazer um pequeno codigo que nos dira isso para finalizar nossa primeira parte
    de manipulacao de dados com o Spark
    '''
    modulo = lambda x: x if x > 0 else -x
    resultado = tempDeviations.map(lambda x: (1, (x[0], x[1][0], x[1][1])) if x[1][0] != 0.0 and x[1][1] != 0.0 else None) \
                              .filter(lambda x: x != None) \
                              .reduceByKey(lambda acc, val: acc if modulo(acc[1] - acc[2]) > modulo(val[1] - val[2]) else val) \
                              .map(lambda x: x[1]) \
                              .collect()
    print resultado