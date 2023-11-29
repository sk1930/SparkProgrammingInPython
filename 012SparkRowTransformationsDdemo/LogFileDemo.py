from pyspark.sql import *
from pyspark.sql.functions import regexp_extract,substring_index

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("LogFileDemo") \
            .getOrCreate()

    filedf = spark.read.text("data/apache_logs.txt")
    filedf.printSchema()
    ''' root
 |-- value: string (nullable = true)
    '''
    log_reg = r'(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    logs_df = filedf.select(regexp_extract('value', log_reg, 1).alias('ip'),
                             regexp_extract('value', log_reg, 4).alias('date'),
                             regexp_extract('value', log_reg, 6).alias('request'),
                             regexp_extract('value', log_reg, 10).alias('referrer'))
    logs_df.show(5,truncate=False)
    '''|83.149.9.216|17/May/2015:10:05:03 +0000|/presentations/logstash-monitorama-2013/images/kibana-search.png     |http://semicomplete.com/presentations/logstash-monitorama-2013/|
'''

    filedf.select(regexp_extract('value', log_reg, 11).alias('new1')).show(truncate=False)
    '''|Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36|
'''



    '''|GET /presentations/logstash-monitorama-2013/images/kibana-search.png HTTP/1.1                  |
        brings the same as above log_reg also ((\S+) (\S+) (\S+))
        in log_reg they r split into 3 -- 5,6,7 indices as they are not in same ()
        
        '''
    log_reg1 = r'(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+\s\S+\s\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    filedf.select(regexp_extract('value', log_reg1, 5).alias('new2')).show(truncate=False)
    ''' |GET /presentations/logstash-monitorama-2013/images/kibana-search.png HTTP/1.1                  |
'''


    logs_df.printSchema()
    ''' now the data has a schema with 4 columns 
    root
 |-- ip: string (nullable = true)
 |-- date: string (nullable = true)
 |-- request: string (nullable = true)
 |-- referrer: string (nullable = true)'''


    logs_df.groupBy("referrer") \
    .count() \
    .show(5,truncate=False)
    '''+-----------------------------------------------------------------------------------------------+-----+
|referrer                                                                                       |count|
+-----------------------------------------------------------------------------------------------+-----+
|http://www.semicomplete.com/blog/tags/wifi                                                     |1    |
|http://www.semicomplete.com/projects/keynav/                                                   |65   |
'''
    logs_df \
        .withColumn("referrer",substring_index("referrer","/",3)) \
        .groupBy("referrer") \
        .count() \
        .show(5,truncate=False)
    '''|http://www.semicomplete.com |  2|
'''
    # there are lot of records with referrer = '-' remove that records with trim
    logs_df \
        .where(trim("referrer") != '-') \
        .withColumn("referrer",substring_index("referrer","/",3)) \
        .groupBy("referrer") \
        .count() \
        .show(5,truncate=False)


