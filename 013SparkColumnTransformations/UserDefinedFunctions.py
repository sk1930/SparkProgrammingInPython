import re
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4J


def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    '''
The string is a single character f

It contains f followed by some character and letter m.

It contains w followed by some other letter and m.'''
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("UDF Demo") \
        .master("local[2]") \
        .getOrCreate()

    #logger = Log4J(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/survey.csv")

    survey_df.show(10)

    parse_gender_udf = udf(parse_gender,StringType())
    print("catalog entry willl not show the udf at this step as this will not register in the catalog")
    [print(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]

    survey_df2 = survey_df.withColumn("Gender",parse_gender_udf("Gender"))
    survey_df2.show()


    # below step creates an entry in the catalog. this is for sql expression
    spark.udf.register("parse_gender_udf",parse_gender,StringType())
    print("catalog entry will show the udf at this step as this will register in the catalog")
    [print(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df3 = survey_df.withColumn("Gender",expr("parse_gender_udf(Gender)"))
    survey_df3.show()





