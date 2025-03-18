from pyspark import SparkContext
from pyspark.sql import SparkSession

data = [
    {"id":1,"name":"Phuong"},
    {"id": 2, "name": "Hoang"},
    {"id": 3, "name": "Nam"},
]

spark = SparkSession.builder \
    .appName("ObjectTransformation") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .master("local[*]")\
    .getOrCreate()

sc = spark.sparkContext
num = [1,2,3,4,5,6,7]

rdd = sc.parallelize(num)
flatmapRdd = rdd.map(lambda x: [x,x*2])
# print(rdd.getNumPartitions())
print(flatmapRdd.collect())