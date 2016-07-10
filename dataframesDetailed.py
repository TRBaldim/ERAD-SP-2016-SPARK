'''
Created on 10/07/2016

@author: thiago
'''

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import tempfile

if __name__ == '__main__':
    byCountry = "/home/thiago/Projetos_Pessoais/ERAD/ERAD-SP-2016-SPARK/GlobalLandTemperatures/GlobalLandTemperaturesByCountry.csv"
    byCountry = 'GlobalLandTemperatures/GlobalLandTemperaturesByCountry.csv'
    byGlobal = 'GlobalLandTemperatures/GlobalTemperatures.csv'


    sc = SparkContext(master='local', appName='ERAD-SP')
        
    '''
    Para trabalharmos com SQLContext devemos criar um contexto SQL
    baseado no SparkContext
    '''    
    
    sqlContext = SQLContext(sc)
    
    '''
    Seguimos com o load dos dados utilizando a ferramenta text file.
    
    Uma forma opcional de fazer o load dos dados e utilizando o comando:
        
        sqlContext.read.format("com.databricks.spark.csv")
                       .option("header", "true")
                       .option("delimiter", ",")
                       .load(PATH_TO_FILE)
    
    Por nao fazer parte da biblioteca nativa do SPARK nao vamos abordar neste
    curso
    '''
    
    ClimateRdd = sc.textFile(byCountry, use_unicode=False) \
                   .map(lambda x: x.split(','))
    
    header = ClimateRdd.first()
    
    ClimateRdd = ClimateRdd.filter(lambda line: line != header)\
                           .map(lambda x: (x[0], x[1], x[2], x[3]))
    
    '''
    ClimateRdd = ClimateRdd.filter(lambda line: line != header)\
                           .map(lambda x: (x[0], x[1], x[2], x[3]))
    print ClimateRdd1.first()
    
    ClimateRdd = ClimateRdd.filter(lambda line: line != header)\
                           .map(lambda x: tuple(x))
    
    print ClimateRdd.first()
    
    
    Como com o RDD e um serio problema gerenciar suas acoes com base apenas no index
    das colunas. O sqlContext pode trabalhar com o nome do header para as colunas.
    Para isso criamos um sqlContext que pode receber um schema para gerar um dataFrame.
    
    Entao passamos um RDD + uma lista que representa o Schema do RDD
    '''
    #print header
    
    df = sqlContext.createDataFrame(ClimateRdd, header)
    
    '''
    Usamos o comando take para retornar a quantidade de valores que desejamos do DF
    para o driver.
    
    Voce pode ver que o retorno e um array de objetos do tipo Row:
    
    Row(dt=u'1743-11-01', AverageTemperature=u'4.3839999999999995', AverageTemperatureUncertainty=u'2.294', Country=u'\xc3\x85land')
    '''
    
    #print df.take(5)
    #df.show(5)
    
    '''
    Outra forma de garantir que o Schema seja mais bem estruturado e com a aplicacao de
    um structType, que e uma classe de struct fields. Que sao usados para criacao de tabelas
    tanto dentro do spark quanto fora, como bancos relacionais ou bancos nao relacionais como Hive.
    A grande vantagem e que garante que erros no dataframe como tipos que nao pertencem ao grupo
    ja sao tratados dentro do codigo e nao no banco.
    '''
    
    fields = [StructField(field_name, StringType(), True) for field_name in header]
    schema = StructType(fields)
    
    #print schema
    
    schemaDf = sqlContext.createDataFrame(ClimateRdd, schema)
    
    #schemaDf.show(5)
    
    '''
    Desta forma conseguimos criar uma tabela temporaria no spark para fazer
    queries para solucoes que linguagem SQL poderia solucionar muito bem.
    E facilitar a vida de quem nao e muito familiarizado com linguagem funcional
    '''
    
    schemaDf.registerTempTable('globalTemp')
    
    
    '''
    Com a criacao de uma tabela temporaria podemos executar queries da seguinte forma:
        sqlContext.sql('SQL_QUERY')
    '''
    
    result = sqlContext.sql("SELECT dt FROM globalTemp")
    
    result.show(5)
    
    #date = result.map(lambda x: x)
    
    #print date.collect()
    '''
    Resultados de uma query no dataFrame resulta em um dataFrame com todas
    as operacoes de um RDD. Com a vantagem de poder selecionar o objeto pelo nome do
    schema.
    '''
    
    dates = result.map(lambda x: "Datas: " + x.dt)
    
    #for date in dates.collect():
        #print date
    
    
    '''
    Para salvar um DF, os comando podem seguir sequencia de acoes
    neste caso abaixo temos o df o comando write para salvar no File System
    o tipo de arquivo neste caso parquet. Que e o arquivo de compressao do hadoop
    '''
    df.write.mode('append').parquet(os.path.join(tempfile.mkdtemp(), 'teste'))
    
    '''
    Segue abaixo um exemplo de conexao com banco de dados.
    
    Para execudcao de banco de dados devemos usar primeiro a definicao do SPAR_CLASSPATH
    
    Pode ser definido no arquivo de configuracao do spark ou diretamente na chamada do 
    spark-shell ou spark-submit como a seguir:
    
        SPARK_CLASSPATH=postgresql-9.3-1102-jdbc41.jar bin/spark-shell
        
    Para criar um daframe com SQLContext devemos usar as funcoes:
        format: para falar qual o formato da conexao, em geral e jdbc
        options: onde devemos retornar os dados de url, db_table
        load: para darmos load no dataFrame
    '''
    
    df = sqlContext.read.format('jdbc').options(url='jdb:postgresql:localhost', dbtable='myDb.table').load()
    
    ############################################
    #        ATENCAO!!!!!!!!!!!                #
    ############################################
    '''
    Ao executar comandos por este df, lembrese que os selects iniciais serao enviados
    diretamente ao banco de dados, e nao serao feitos via spark.
    Caso haja lentidao pode ser resultado do cluster do banco de dados.
    '''
    
    
    
    