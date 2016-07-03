'''
Created on 02/07/2016

@author: thiago
'''

from pyspark import SparkContext
from plottinExemples import plotDates


if __name__ == '__main__':
    
    byCountry = 'GlobalLandTemperatures/GlobalLandTemperaturesByCountry.csv'
    byCity = 'GlobalLandTemperatures/GlobalLandTemperaturesByCity.csv'
    byMajorCity = 'GlobalLandTemperatures/GlobalLandTemperaturesByMajorCity.csv'
    byState = 'GlobalLandTemperatures/GlobalLandTemperaturesByState.csv'
    byGlobal = 'GlobalLandTemperatures/GlobalTemperatures.csv'
    
    
    
    '''
    Em detalhes segue a configuracao de cada aspecto dos parametros para o SparkContext
    
        :param master: Cluster URL to connect to
               (e.g. mesos://host:port, spark://host:port, local[4]).
        :param appName: A name for your job, to display on the cluster web UI.
        :param sparkHome: Location where Spark is installed on cluster nodes.
        :param pyFiles: Collection of .zip or .py files to send to the cluster
               and add to PYTHONPATH.  These can be paths on the local file
               system or HDFS, HTTP, HTTPS, or FTP URLs.
        :param environment: A dictionary of environment variables to set on
               worker nodes.
        :param batchSize: The number of Python objects represented as a single
               Java object. Set 1 to disable batching, 0 to automatically choose
               the batch size based on object sizes, or -1 to use an unlimited
               batch size
        :param serializer: The serializer for RDDs.
        :param conf: A L{SparkConf} object setting Spark properties.
        :param gateway: Use an existing gateway and JVM, otherwise a new JVM
               will be instantiated.
        :param jsc: The JavaSparkContext instance (optional).
        :param profiler_cls: A class of custom Profiler used to do profiling
               (default is pyspark.profiler.BasicProfiler).
    
    SparkContext(master, appName, sparkHome, pyFiles, environment, batchSize, serializer, conf, gateway, jsc, profiler_cls)
    
    '''
    
    # Para aspectos de testes usaremos apenas appName, pois nao estamos usando clusters e estamos apenas utilizando execucao local.
    sc = SparkContext(master='local', appName='ERAD-SP')
    
    '''
    O comando:
        sc.textFile(name, minPartitions, use_unicode)
    Representa uma entrada essencial para o desenvolvimento do Spark. Nos Aspectos de RDD (Classes Primitivas do Spark)
    Temos como definitivo um elemento de importacao name: reponsavel por importar elementos tanto do Hadoop quanto de FileSystems locais.
    '''
    
    ClimateRdd = sc.textFile(byCountry, use_unicode=False)
    
    #print ClimateRdd.collect()[:10]
    
    #Primeiro mapping do RDD, voltado para alterar o tipo de seralizacao. Criando varios elemntos por linha
    #Alterando os arquivos de string 'a, b, c' para arrays de strings ['a', 'b', 'c']
    ClimateRdd = ClimateRdd.map(lambda line: line.split(','))
    
    #print ClimateRdd.collect()[:10]
    
    #Retorna o primeiro elemento do RDD. Neste caso o Header
    header = ClimateRdd.first()
    
    #print header
    
    #Remove o header do RDD para nao afetar os resultados
    ClimateRdd = ClimateRdd.filter(lambda line: line != header)
    
    
    '''
    Para trabalhar com o RDD devemos usar um sistema de Indexes. Como em arrays. O nosso RDD ClimateRDD
    tem como referencia os dados do arquvivo: GlobalLandTemperaturesByCountry.csv.
    O header tem essa estrutura:
    
    dt,AverageTemperature,AverageTemperatureUncertainty,Country
    
    Que para ajustes de index temos:
    
    dt = [0]
    ...
    Country = [3]
    
    Estes serao os indices que devem ser usados.
    
    Neste caso vamos fazer uma comparacao das medias de cada pais com o dataSet da temperatura global 
    '''
    
    #primeiro passo sera separar as informacoes de data e temperatura
    #Indices [0] e [1]
    #O retorno sera uma array de (K, V) onde a key sera a data e o value sera a temperatura
    #Ja podemos fazer o casting dos dados de texto para float.
    
    #ClimateRdd = ClimateRdd.map(lambda x: (x[0], float(x[1])))
    
    #Como possuimos um valor que pode variar entre vazio e valor em float.
    #Devemos fazer um ajuste para garantir que o casting seja feito com sucesso.
    
    DateTempRdd = ClimateRdd.map(lambda x: (x[0], float(x[1]) if x[1] != '' else 0.0))
    
    #print ClimateRdd.collect()[:10]
    
    '''
    Com a estrutra de map reduce do Spark devemos tentar reduzir as datas para de acorodo com
    o que necessitamos, neste caso queremos juntar todas as datas para tirarmos as medias de
    temperatura dos anos que se passaram.
    '''    
    #Neste momento estamos somando todos os valores e nao conseguimos calcular a media dos valores.
    #como calcular a media?
    ClimateReduced = DateTempRdd.reduceByKey(lambda val, acc: val + acc)
    
    #print ClimateReduced.collect()[:10]
    
    
    '''
    Neste caso vamos refazer o DateTempRdd.
    O DateTempRdd devera receber (K, V). Mas nesse caso o (K, V) nao sera apenas o valor da temperatura.
    Mas junto tambem teremos um valor para auxiliar a contagem de elementos reduzidos.
    Logo no (K, V) sera mais ou menos assim: (K, V) => (K, (V, V)).
    '''
    #Neste caso estamos tendo uma estrutura de RDD que possui: (Data, (Temperatura, 1))
    DateTempRdd = ClimateRdd.map(lambda x: (x[0], (float(x[1] if x[1] != '' else 0.0), 1)))
    
    #print DateTempRdd.collect()[:10]
    
    '''
    Agora com a variavel auxiliar podemos fazer a contagem da media.
    Para isso teremos que fazer um calculo que seja equivalente a estrutura dada anterirormente.
    (K, (V, V))
    '''
    #Neste caso vamos receber um valor (float, int) devemos devolver da mesma forma (float,  int)
    #Assim estamos somando (1.1+2.1, 1+1)
    ClimateReduced = DateTempRdd.reduceByKey(lambda val, acc: (val[0] + acc[0], val[1] + acc[1]))
    
    #print ClimateReduced.collect()[:10]
    
    #Agora usamos o Map para fazer a media.
    #Nesse momento temos um elemento que possui este formato: (Date, (Sum_Temp, Sum_count))
    #Queremos um valor que seja: (Date, (Avg_Temp))
    CityByDate = ClimateReduced.map(lambda x: (x[0], x[1][0] / x[1][1]))
    
    #print CityByDate.collect()[:10]
    
    '''
    Apos o resultado final podemos ordenar o resultado por data.
    Para isso podemos usar a funcao sortBy. Que podemos ordenar pela coluna selecionada.
    '''
    
    CityByDate = CityByDate.sortBy(lambda x: x[0])
    
    #print CityByDate.collect()[:10]
    
    '''
    Neste primeiro exemplo fizemos tudo passo a passo. Mas todas essas manipulacoes
    poderiam ter sido feita de uma so vez. Como a seguir.
    A partir de agora as funcoes que ja foram vistas serao feitas de uma so vez.
    Neste caso vamos comecar a fazer apos a remocao do header.
    '''
    
    CityByDate =  ClimateRdd.map(lambda x: (x[0], (float(x[1] if x[1] != '' else 0.0), 1))) \
                            .reduceByKey(lambda val, acc: (val[0] + acc[0], val[1] + acc[1])) \
                            .map(lambda x: (x[0], x[1][0] / x[1][1])) \
                            .sortBy(lambda x: x[0])
    
    #print CityByDate.collect()[:10]
    
    
    '''
    A seguir temos algumas funcoes interessantes para o RDD.
    Que podem ajudar muito na vida do desenvolvedor, ou do cientista de dados para informacoes estatisticas
    '''
    #Contagem de elementos RDD
    #print CityByDate.count()
    
    #Contagem do numero de cada elemento para cada key Retorno (Key, Count)
    #print ClimateRdd.countByKey().items()
    
    '''
    Voce podera verificar que o comando:
        print ClimateRdd.countByValue()
    Nao ira funcionar, este elemento retornara em uma exception deste tipo:
        TypeError: unhashable type: 'list'
    Isto ocorre pois o elemento do value e um tipo lista: (Date, [Other Values]
    Este tipo de elemento, nao e possivel de criar um Hash Code para se fazer o map Reduce
    baseando na estrutura de dict do python, pois uma list e mutavel.
    Uma forma de solucionar este problema e fazendo um map convertendo o value de lista para tuple:
        print ClimateRdd.map(lambda x: (x[0], (x[1], x[2], x[3]))).countByValue()
    '''
    
    #Contagem do numero de cada elemento para cada Valor. Retorno ((Key, Value), Count)
    #print ClimateRdd.map(lambda x: (x[0], (x[1], x[2], x[3]))).countByValue().items()[:10]
    
    #Retorno de Elementos Distintos
    #print CityByDate.distinct().collect()[:10]
    
    #Retorna um sample do RDD, em geral para testes.
    #sample(withReplacement, fraction, seed)
    #withReplacement: pode ter elementos repetidos caso verdadeiro
    #fraction: fracao dos dados a serem pegos
    #seed: semente para a randomizacao
    #print CityByDate.sample(False, 0.1, 42)
    
    test = plotDates()
    test.addData(CityByDate.collect())
    
    
                         
    
    