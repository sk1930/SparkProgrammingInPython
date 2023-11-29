
import sys
from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4J
from lib.utils import get_spark_app_config,load_survey_df

if __name__ == "__main__":
    conf = get_spark_app_config()
    print("Starting hello spark")
    spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()



    flightTimeParquetDF = spark.read \
            .format("parquet") \
            .load("data/flight*.parquet")

    flightTimeParquetDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path","dataSink/avro/") \
        .save()

    print("Num partitions before")
    print(flightTimeParquetDF.rdd.getNumPartitions())   # 2


    flightTimeParquetDF.groupby(spark_partition_id())  \
        .count() \
        .show()

    '''
    +--------------------+------+
|SPARK_PARTITION_ID()| count|
+--------------------+------+
|                   0|470477|
+--------------------+------+

'''

    partitioned_DF = flightTimeParquetDF.repartition(5)
    print("Num partitions after")
    print(partitioned_DF.rdd.getNumPartitions())  # 5
    '''
    +--------------------+-----+
|SPARK_PARTITION_ID()|count|
+--------------------+-----+
|                   0|94096|
|                   1|94095|
|                   2|94095|
|                   3|94095|
|                   4|94096|
+--------------------+-----+
'''

    partitioned_DF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/avro1/") \
        .save()


    partitioned_DF.groupby(spark_partition_id()) \
        .count() \
        .show()


    flightTimeParquetDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path","dataSink/json/") \
        .partitionBy("OP_CARRIER","origin") \
        .save()

    # with max records per file to control the file size
    # see OP_CARRIER =DL and origin = ATL
    flightTimeParquetDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataSink/json1/") \
        .partitionBy("OP_CARRIER", "origin") \
        .option("maxRecordsPerFile",10000) \
        .save()




    print("Starting hello spark2")
    logger = Log4J(spark)
    logger.info(conf_out.toDebugString())

    spark.stop()
