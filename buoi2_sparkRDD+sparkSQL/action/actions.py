from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("KeyValue") \
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .getOrCreate()
sc = spark.sparkContext

rdd1 = sc.parallelize([(130,50.5),(120,12.2),(150,50.0),(150,27.6),(160,28.9),(130,20.2)])
# print(rdd1.lookup(110))
print(dict(rdd1.countByKey()))