

from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    conf = get_spark_app_config()
    print("Starting hello spark")
    spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


    conf_out = spark.sparkContext.getConf()
    print(conf_out.toDebugString())
    ''' it prints everything here by while passing to spark --- the spark session ignore the
    Warning: Ignoring non-Spark config property: app.author
Warning: Ignoring non-Spark config property: abcd

'''
    print("Starting hello spark2")
    logger = Log4J(spark)
    logger.info(conf_out.toDebugString())

    spark.stop()
