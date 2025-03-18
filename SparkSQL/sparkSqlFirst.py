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

rdd = sc.parallelize(range(1,11)).map(lambda x:(x,random.randint(0,99)*x))
df = spark.createDataFrame(rdd,schema=['key','value'])
df.show()