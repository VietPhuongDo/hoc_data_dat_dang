import random

from pyspark import SparkConf, SparkContext, Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("KeyValue") \
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .config("spark.executor.memory","4g")\
    .getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([
    Row(1,'pham thoai','undefine'),
    Row(2,'hang du muc','women'),
    Row(3,'Quang linh','men')
])

schema = StructType([
    StructField("id",IntegerType(),nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("sex", StringType(), nullable=True)
])

df = spark.createDataFrame(data=rdd,schema=schema)
df.show()