
'''
notes in 013 Creating Spark Project Build Configuration'''

from pyspark.sql import *

from lib.logger import Log4J

if __name__ == "__main__":
    print("Starting hello spark")
    spark = SparkSession.builder \
    .appName("Hello Spark") \
    .master("local[3]") \
    .getOrCreate()
    print("Starting hello spark2")

    logger = Log4J(spark)
    print("Starting hello spark3")

    logger.info("Starting hello spark")
    logger.info("Finished  hello spark")


    spark.stop()
