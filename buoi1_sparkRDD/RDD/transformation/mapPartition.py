import time
from random import Random
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ObjectTransformation") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .master("local[*]")\
    .getOrCreate()

sc = spark.sparkContext

data = ["aabb-debt","bbcc-debt","ccdd-debt","ddee-debt"]
rdd = sc.parallelize(data,numSlices=2)

# def partitionOn(iterator):
#     rand = Random(int(time.time() * 1000) + Random().randint(0,1000))
#     return [f"{item}:{rand.randint(1,1000)}" for item in iterator]
#
# result = rdd.mapPartitions(partitionOn)
# print(result.collect())

results = rdd.mapPartitions(
    lambda index:map(
        lambda l: f"{l}:{Random(int(time.time() * 1000) + Random().randint(0,1000)).randint(0,1000)}",
        index
    )
)
print(results.collect())