import sys
from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df

if __name__ == "__main__":
    conf = get_spark_app_config()
    print("Starting hello spark")
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()


    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")
    flightTimeParquetDF.show(5)
    print(flightTimeParquetDF.schema)
    # or
    # print(flightTimeCsvDF.schema.simpleString())
    '''+----------+----------+-----------------+------+----------------+----+--------------+------------+--------+---------+-------+------------+--------+---------+--------+
|   FL_DATE|OP_CARRIER|OP_CARRIER_FL_NUM|ORIGIN|ORIGIN_CITY_NAME|DEST|DEST_CITY_NAME|CRS_DEP_TIME|DEP_TIME|WHEELS_ON|TAXI_IN|CRS_ARR_TIME|ARR_TIME|CANCELLED|DISTANCE|
+----------+----------+-----------------+------+----------------+----+--------------+------------+--------+---------+-------+------------+--------+---------+--------+
|2000-01-01|        DL|             1451|   BOS|      Boston, MA| ATL|   Atlanta, GA|        1115|    1113|     1343|      5|        1400|    1348|        0|     946|
|2000-01-01|        DL|             1479|   BOS|      Boston, MA| ATL|   Atlanta, GA|        1315|    1311|     1536|      7|        1559|    1543|        0|     946|

StructType(List(StructField(FL_DATE,DateType,true),StructField(OP_CARRIER,StringType,true),StructField(OP_CARRIER_FL_NUM,IntegerType,true),StructField(ORIGIN,StringType,true),StructField(ORIGIN_CITY_NAME,StringType,true),StructField(DEST,StringType,true),StructField(DEST_CITY_NAME,StringType,true),StructField(CRS_DEP_TIME,IntegerType,true),StructField(DEP_TIME,IntegerType,true),StructField(WHEELS_ON,IntegerType,true),StructField(TAXI_IN,IntegerType,true),StructField(CRS_ARR_TIME,IntegerType,true),StructField(ARR_TIME,IntegerType,true),StructField(CANCELLED,IntegerType,true),StructField(DISTANCE,IntegerType,true)))

'''

    print("Starting hello spark2")
    logger = Log4J(spark)
    logger.info(conf_out.toDebugString())

    spark.stop()
