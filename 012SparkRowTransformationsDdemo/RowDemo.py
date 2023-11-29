
import sys
from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id, to_date
from pyspark.sql.types import StructType, StructField, StringType

from lib.logger import Log4J
from lib.utils import get_spark_app_config,load_survey_df

def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(df[fld], fmt))


if __name__ == "__main__":

    print("Starting hello spark")
    spark = SparkSession.builder \
    .master("local[3]") \
    .appName("RowDemo") \
    .getOrCreate()

    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())])

    my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
    my_rdd = spark.sparkContext.parallelize(my_rows, 2)
    my_df = spark.createDataFrame(my_rdd, my_schema)

    my_df.printSchema()
    my_df.show()

    print("\n After \n")
    new_df = to_date_df(my_df, "M/d/y", "EventDate")
    new_df.printSchema()
    new_df.show()

    print("Starting hello spark2")
    logger = Log4J(spark)
    logger.info(conf_out.toDebugString())

    spark.stop()
