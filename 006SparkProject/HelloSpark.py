
import sys
from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config,load_survey_df,count_by_country

if __name__ == "__main__":
    conf = get_spark_app_config()
    print("Starting hello spark")
    spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


    conf_out = spark.sparkContext.getConf()
    if len(sys.argv) != 2:
        # logger.error("Usage: HelloSpark <filename>")
        print( "Usage: HelloSpark <filename>")
        '''
click on edit configuration
there is a script box
below that is the script parameters

there enter data/sample.csv'''
        sys.exit(-1)

    surveyDF = load_survey_df(spark,sys.argv[1])
    partitioned_survey_df = surveyDF.repartition(2)
    count_df = count_by_country(partitioned_survey_df)


    print(count_df.collect())


    print("Starting hello spark2")
    input("press enter this is to hold the spark UI ")

    logger = Log4J(spark)
    logger.info(conf_out.toDebugString())

    spark.stop()
