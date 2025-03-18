from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("KeyValue") \
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .getOrCreate()
sc = spark.sparkContext
rdd = sc.parallelize(["phuong","hoang","diep","manh"])
mappedRdd = rdd.map(lambda x:(len(x),x))
groupedRdd = mappedRdd.groupByKey()
for key,value in groupedRdd.collect():
    print(key,list(value))