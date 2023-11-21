
import sys
from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config,load_survey_df

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
        sys.exit(-1)

    surveyDF = load_survey_df(spark,sys.argv[1])
    surveyDF.show()


    print("Starting hello spark2")
    logger = Log4J(spark)
    logger.info(conf_out.toDebugString())

    spark.stop()
