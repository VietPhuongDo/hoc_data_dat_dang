from PIL.ImImagePlugin import number
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import re


spark = SparkSession.builder.appName("Date Extraction").getOrCreate()

data = [("11/12/2025",),
        ("27/02.2014",),
        ("2023.01.09",),
        ("28-12-2005",)]
df = spark.createDataFrame(data, ["date"])

def get_dateMonthYear(date_str):
    numbers = re.findall(r'\d+',date_str)

    if len(numbers[0])==4:
        year = numbers[0]
        month = numbers[1]
        day = numbers[2]
    if len(numbers[0])==2:
        day = numbers[0]
        month = numbers[1]
        year = numbers[2]

    return int(day),int(month),int(year)

udf_parseDate = udf(get_dateMonthYear, StructType([
    StructField('day',IntegerType(),False),
    StructField('month', IntegerType(), False),
    StructField('year', IntegerType(), False)
]))

df = df.withColumn("parse",udf_parseDate(df['date']))
df.selectExpr('date', 'parse.day', 'parse.month', 'parse.year').show()

