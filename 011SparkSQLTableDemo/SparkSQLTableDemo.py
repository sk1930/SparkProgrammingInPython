
import sys
from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4J
from lib.utils import get_spark_app_config,load_survey_df

if __name__ == "__main__":

    print("Starting hello spark")
    spark = SparkSession.builder \
    .master("local[3]") \
    .appName("SparkSQLTableDemo") \
    .enableHiveSupport() \
    .getOrCreate()



    flightTimeParquetDF = spark.read \
            .format("parquet") \
            .load("data/flight*.parquet")


    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")


    '''this will save the table in the default database, the default database name itself is default
    flightTimeParquetDF.write \
        .mode("overwrite") \
        .saveAsTable("flight_data_tbl")
        '''

    '''if i want to save to AIRLINE_DB u have 2 options
    option1 set 
    spark.catalog.setCurrentDatabase("AIRLINE_DB")
    
    or 
    
    .saveAsTable("AIRLINE_DB.flight_data_tbl")
    
    
    '''
    spark.catalog.setCurrentDatabase("AIRLINE_DB")
    flightTimeParquetDF.write \
        .mode("overwrite") \
        .partitionBy("OP_CARRIER","ORIGIN") \
        .saveAsTable("flight_data_tbl")


    '''bucketBy() allows you to restrict the number of partitions. I am limiting it to five. So, these five partitions are now called buckets.'''

    '''I am changing the format to CSV. Parquet is a binary format, and it is the recommended format. However, I want to manually investigate the data, so I am changing it to CSV.'''
    flightTimeParquetDF.write \
        .mode("overwrite") \
        .format("csv") \
        .bucketBy(5, "OP_CARRIER","ORIGIN") \
        .saveAsTable("flight_data_tbl1")

    ''' However, if these records are sorted, they could be much more ready to use for certain operations. Right? So the bucketBy() can also have a sortBy() companion.
    '''

    flightTimeParquetDF.write \
        .mode("overwrite") \
        .format("csv") \
        .bucketBy(5, "OP_CARRIER","ORIGIN") \
        .sortBy("OP_CARRIER","ORIGIN") \
        .saveAsTable("flight_data_tbl2")


    print(spark.catalog.listTables("AIRLINE_DB"))

    print("Starting hello spark2")
    logger = Log4J(spark)
    logger.info(conf_out.toDebugString())

    spark.stop()
