
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




    '''flightTimeCsvDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .load("data/flight*.csv")
    flightTimeCsvDF.show(5)
    print(flightTimeCsvDF.schema)
    # or
    # print(flightTimeCsvDF.schema.simpleString())

'''
    '''
    I didnt use inferSchema as true -- so all columns are of string type here
    | FL_DATE|OP_CARRIER|OP_CARRIER_FL_NUM|ORIGIN|ORIGIN_CITY_NAME|DEST|DEST_CITY_NAME|CRS_DEP_TIME|DEP_TIME|WHEELS_ON|TAXI_IN|CRS_ARR_TIME|ARR_TIME|CANCELLED|DISTANCE|
+--------+----------+-----------------+------+----------------+----+--------------+------------+--------+---------+-------+------------+--------+---------+--------+
|1/1/2000|        DL|             1451|   BOS|      Boston, MA| ATL|   Atlanta, GA|        1115|    1113|     1343|      5|        1400|    1348|        0|     946|
|1/1/2000|        DL|             1479|   BOS|      Boston, MA| ATL|   Atlanta, GA|        1315|    1311|     1536|      7|        1559|    1543|        0|     946|

StructType(List(StructField(FL_DATE,StringType,true),StructField(OP_CARRIER,StringType,true),StructField(OP_CARRIER_FL_NUM,StringType,true),StructField(ORIGIN,StringType,true),StructField(ORIGIN_CITY_NAME,StringType,true),StructField(DEST,StringType,true),StructField(DEST_CITY_NAME,StringType,true),StructField(CRS_DEP_TIME,StringType,true),StructField(DEP_TIME,StringType,true),StructField(WHEELS_ON,StringType,true),StructField(TAXI_IN,StringType,true),StructField(CRS_ARR_TIME,StringType,true),StructField(ARR_TIME,StringType,true),StructField(CANCELLED,StringType,true),StructField(DISTANCE,StringType,true)))

'''
    flightTimeCsvDF = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema","true") \
            .load("data/flight*.csv")
    flightTimeCsvDF.show(5)
    print(flightTimeCsvDF.schema)
        # or
        # print(flightTimeCsvDF.schema.simpleString())

    '''
    
    StructType(List(StructField(FL_DATE,StringType,true),StructField(OP_CARRIER,StringType,true),StructField(OP_CARRIER_FL_NUM,IntegerType,true),StructField(ORIGIN,StringType,true),StructField(ORIGIN_CITY_NAME,StringType,true),StructField(DEST,StringType,true),StructField(DEST_CITY_NAME,StringType,true),StructField(CRS_DEP_TIME,IntegerType,true),StructField(DEP_TIME,IntegerType,true),StructField(WHEELS_ON,IntegerType,true),StructField(TAXI_IN,IntegerType,true),StructField(CRS_ARR_TIME,IntegerType,true),StructField(ARR_TIME,IntegerType,true),StructField(CANCELLED,IntegerType,true),StructField(DISTANCE,IntegerType,true)))

    Now it is a little better. My numeric fields are now inferred to be an integer. That's what I wanted. However, my date filed is still a string. So the point is straight. You cannot rely on the infer schema option. So you have got only two options. Explicitly specify a schema Or use a data file format which comes with the schema'''







    print("Starting hello spark2")
    logger = Log4J(spark)
    logger.info(conf_out.toDebugString())

    spark.stop()
