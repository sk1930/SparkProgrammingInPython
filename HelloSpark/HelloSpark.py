# open pycharm
# select a location for the project
# C:\Users\Student\Desktop\Personal\Spark\HelloSpark
# uncheck creation of main.py file
# click on create
# Under HelloSpark Folder create HelloSpark.py
# to the bottom there is python version -- click on that
# there will be manage packages - installed  list
# if u don't see pyspark installed --search and install it'''

from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("Hello spark") \
            .master("local[2]")\
            .getOrCreate()
    '''So here is a configuration that tells about the target cluster manager. I used local[3]. In this case, Spark runs locally as a multi-threaded application. In the case of my example, I configured it to start three threads, and that's what the three means. You can have 2, 3, 5, or whatever number you want. If you simply say local and do not give any number, then it becomes a single-threaded application. So, when you configure your application to run with a single local thread, then you will have a driver only and no executors. And in that case, your driver is forced to do everything itself. Nothing happens in parallel. However, when you run your application with three local threads, then your application will have one dedicated thread for the driver and two executor threads. And this local cluster manager is designed for running, testing, and debugging your spark application locally. This technique is nothing but a simulation of distributed client-server architecture using multiple threads so we can test our application locally.
'''
    data_list = [("Ravi",28),
                 ("David",45)]
    df = spark.createDataFrame(data_list).toDF("Name","Age")
    df.show()