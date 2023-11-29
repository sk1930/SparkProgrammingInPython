import re
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4J


if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("MiscDemo") \
            .getOrCreate()
    data_list = [("Ravi", 28, 1, 2002),
             ("Abdul", 23, 5, 81), # 1981
             ("John", 12, 12, 6), # 2006
             ("Rosy", 7, 8,63), # 1963
             ("Abdul", 23, 5, 81) # 1981
            ]
    raw_df = spark.createDataFrame(data_list)#.toDF("name", "day", "month", "year").repartition(3)
    raw_df.printSchema()
    '''root
 |-- _1: string (nullable = true)
 |-- _2: long (nullable = true)
 |-- _3: long (nullable = true)
 |-- _4: long (nullable = true)'''


    raw_df1 = spark.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)
    raw_df1.printSchema()


    '''root
 |-- name: string (nullable = true)
 |-- day: long (nullable = true)
 |-- month: long (nullable = true)
 |-- year: long (nullable = true)'''
    data_list1 = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")  # 1981
                 ]
    raw_df1 = spark.createDataFrame(data_list1).toDF("name", "day", "month", "year").repartition(3)
    raw_df1.printSchema()
    '''data_list = [("Ravi", "28", "1", "2002"),
             ("Abdul", "23", "5", "81"), # 1981
             ("John", "12", "12", "6"), # 2006
             ("Rosy", "7", "8", "63"), # 1963
             ("Abdul", "23", "5", "81") # 1981
            ]
raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)
raw_df.printSchema()'''






    # monotonically increasing id across all partitions
    # it need not be consecutive numbers
    raw_df2 = raw_df1.repartition(3)
    df1 = raw_df2.withColumn("id",monotonically_increasing_id())
    df1.show()
    '''
+-----+---+-----+----+-----------+
| name|day|month|year|         id|
+-----+---+-----+----+-----------+
| Ravi| 28|    1|2002|          0|
|Abdul| 23|    5|  81|          1|
|Abdul| 23|    5|  81| 8589934592|
| John| 12|   12|   6|17179869184|
| Rosy|  7|    8|  63|17179869185|
+-----+---+-----+----+-----------+
'''

# case when then
# Let's assume that the year between 0 to 20 has an intention to be a post-2000 year. And anything between 21 to 99 is in the previous century.
    df2 = df1.withColumn("year",expr("""case when year <21 then year +2000
                                     when year<100 then year +1900
                                    else year
                                end"""))

    df2.show()
    '''+-----+---+-----+------+-----------+
| name|day|month|  year|         id|
+-----+---+-----+------+-----------+
| Ravi| 28|    1|  2002|          0|
|Abdul| 23|    5|1981.0|          1|
|Abdul| 23|    5|1981.0| 8589934592|
| John| 12|   12|2006.0|17179869184|
| Rosy|  7|    8|1963.0|17179869185|
+-----+---+-----+------+-----------+'''


    df2.printSchema()
    '''root
 |-- name: string (nullable = true)
 |-- day: string (nullable = true)
 |-- month: string (nullable = true)
 |-- year: string (nullable = true)
 |-- id: long (nullable = false)'''

    '''The year becomes decimal. This is caused by the incorrect data type and the automatic type promotion. The year field in the data frame is a string. However, we performed an arithmetic operation in the year field. So, the Spark SQL engine automatically promoted it to decimal. And after completing the arithmetic operation, it is again demoted back to string because the data frame schema is for a string field. However, in all this, we still keep the decimal part, and that's how you see a decimal value in the result. How to fix it? There comes the next item. Casting your data frame columns is a common requirement. '''

    '''We have a bunch of ways to do it. However, I am particularly interested in two commonly used approaches. The first option is to make sure that the Spark doesn't automatically promote or demote your field types. Instead, we can write code to push it to an appropriate type. Let me do it. Simple. I am casting the year to an int. Same thing here also. Now the Spark SQL engine does not need to promote it and cause problems for us. This is the recommended approach for casting your fields. Cast it right at the place you want to cast. Let me run it and show you the result. Good. Now you see four-digit years.'''
    df3 = df1.withColumn("year",expr("""case when year <21 then cast(year as int) +2000
                                     when year<100 then cast(year as int) +1900
                                    else year
                                end"""))

    df3.show()
    '''| name|day|month|year|         id|
+-----+---+-----+----+-----------+
| Ravi| 28|    1|2002|          0|
|Abdul| 23|    5|1981|          1|
|Abdul| 23|    5|1981| 8589934592|
| John| 12|   12|2006|17179869184|
| Rosy|  7|    8|1963|17179869185|'''
    df3.printSchema() #  |-- year: string (nullable = true)



    df4 = df1.withColumn("day",df1['day'].cast(IntegerType())) \
                .withColumn("month",col('month').cast(IntegerType())) \
                .withColumn("year", col('year').cast(IntegerType()))

    # or     df4 = df1.withColumn("day",col('day').cast(IntegerType()))
    # seems it is better to use col("day") than df['day] when we write a series of withCOlumns...
    df4.printSchema() #  |-- day: integer (nullable = true)



    # 2nd way of case when then
    df7 = df4.withColumn("year", \
                         when (col("year")<21, col("year")+2000) \
                         .when (col("year")<100,col("year")+1900) \
                         .otherwise(col("year")))

    df7.show()



    # how to add/remove columns and duplicates
    # we have already added id in the beginning
    df8 = df7.withColumn("dob",expr("to_date(concat(day,'/',month,'/',year),'d/M/y')"))
    #or
    df9 = df7.withColumn("dob",to_date(expr("concat(day,'/',month,'/',year)"),'d/M/y')) \
            .drop("day","month","year")

    df9.show()
    '''| name|         id|       dob|
+-----+-----------+----------+
| Ravi|          0|2002-01-28|
|Abdul|          1|1981-05-23|
|Abdul| 8589934592|1981-05-23| '''
    # there are duplicates

    df10 = df9.dropDuplicates(["name","dob"])
    df10.show()


    df11 = df10.sort(expr("dob desc")) # dob desc is not a field name so u make it a expression
    df11.show()


    spark.stop()


