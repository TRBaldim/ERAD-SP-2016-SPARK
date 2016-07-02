'''
Created on 02/07/2016

@author: thiago
'''

from pyspark import SparkContext


if __name__ == '__main__':
    
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
    
    ClimateRdd = sc.textFile('GlobalLandTemperatures/GlobalLandTemperaturesByCountry.csv', use_unicode=False)
    
    print ClimateRdd.collect()[:10]
    
    #Primeiro mapping do RDD, voltado para alterar o tipo de seralizacao. Criando varios elemntos por linha
    #Alterando os arquivos de string 'a, b, c' para arrays de strings ['a', 'b', 'c']
    ClimateRdd = ClimateRdd.map(lambda line: line.split(','))
    
    print ClimateRdd.collect()[:10]
