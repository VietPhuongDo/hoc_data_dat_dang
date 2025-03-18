from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ObjectTransformation") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .master("local[*]")\
    .getOrCreate()

sc = spark.sparkContext

rdd1 = sc.parallelize( [
    {"id":1,"name":"Phuong"},
    {"id": 2, "name": "Hoang"},
    {"id": 3, "name": "Nam"},
])

rdd2 = sc.parallelize([1,2,3,4,5,6,7,8,9])
rdd3 = rdd1.union(rdd2).collect()
print(rdd3)