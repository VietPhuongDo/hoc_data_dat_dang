import random

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("KeyValue") \
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .config("spark.executor.memory","4g")\
    .getOrCreate()
sc = spark.sparkContext

df = spark.read.text()