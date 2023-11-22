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


    flightTimeJsonDF = spark.read \
        .format("json") \
        .load("data/flight*.json")
    flightTimeJsonDF.show(5)
    print(flightTimeJsonDF.schema)
    # or
    # print(flightTimeCsvDF.schema.simpleString())
'''
+--------+---------+------------+------------+--------+----+--------------+--------+--------+----------+-----------------+------+----------------+-------+---------+
|ARR_TIME|CANCELLED|CRS_ARR_TIME|CRS_DEP_TIME|DEP_TIME|DEST|DEST_CITY_NAME|DISTANCE| FL_DATE|OP_CARRIER|OP_CARRIER_FL_NUM|ORIGIN|ORIGIN_CITY_NAME|TAXI_IN|WHEELS_ON|
+--------+---------+------------+------------+--------+----+--------------+--------+--------+----------+-----------------+------+----------------+-------+---------+
|    1348|        0|        1400|        1115|    1113| ATL|   Atlanta, GA|     946|1/1/2000|        DL|             1451|   BOS|      Boston, MA|      5|     1343|
|    1543|        0|        1559|        1315|    1311| ATL|   Atlanta, GA|     946|1/1/2000|        DL|             1479|   BOS|      Boston, MA|      7|     1536|


StructType(List(StructField(ARR_TIME,LongType,true),StructField(CANCELLED,LongType,true),StructField(CRS_ARR_TIME,LongType,true),StructField(CRS_DEP_TIME,LongType,true),StructField(DEP_TIME,LongType,true),StructField(DEST,StringType,true),StructField(DEST_CITY_NAME,StringType,true),StructField(DISTANCE,LongType,true),StructField(FL_DATE,StringType,true),StructField(OP_CARRIER,StringType,true),StructField(OP_CARRIER_FL_NUM,LongType,true),StructField(ORIGIN,StringType,true),StructField(ORIGIN_CITY_NAME,StringType,true),StructField(TAXI_IN,LongType,true),StructField(WHEELS_ON,LongType,true)))
'''
    print("Starting hello spark2")
    logger = Log4J(spark)
    logger.info(conf_out.toDebugString())

    spark.stop()
