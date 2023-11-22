
import sys
from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from lib.logger import Log4J
from lib.utils import get_spark_app_config,load_survey_df,count_by_country



if __name__ == "__main__":
    SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])

    conf = SparkConf() \
    .setMaster("local[3]") \
    .setAppName("HelloRDD")

    '''one way
    sc= SparkContext(conf=conf)
    '''
    spark = SparkSession \
        .builder \
    .config(conf=conf)\
    .getOrCreate()

    sc = spark.sparkContext




    if len(sys.argv) != 2:
        # logger.error("Usage: HelloSpark <filename>")
        print( "Usage: HelloSpark <filename>")
        sys.exit(-1)


        '''
click on edit configuration
there is a script box
below that is the script parameters

there enter data/sample.csv'''

    linesRDD = sc.textFile(sys.argv[1])
    partitioned_RDD = linesRDD.repartition(2)

    colsRDD = partitioned_RDD.map(lambda line: line.replace('"','').split(","))


    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]),cols[2],cols[3],cols[4]))


    filteredRDD = selectRDD.filter(lambda r: r.Age<40)

    keyValueRDD = filteredRDD.map(lambda r:(r.Country,1))
    # it takes each record and country becomes the key and value is 1

    countRDD = keyValueRDD.reduceByKey(lambda v1,v2:v1+v2)

    colsList = countRDD.collect()
    print("cols list is")
    print(colsList)


    logger = Log4J(spark)
    logger.info(conf_out.toDebugString())

    spark.stop()
