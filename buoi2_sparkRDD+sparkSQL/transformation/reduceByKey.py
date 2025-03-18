from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("ReduceKeyValue") \
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .getOrCreate()
sc = spark.sparkContext
rdd = sc.parallelize([("phuong",15.2),("hoang",15.3),("diep",15.4),("manh",15.5),("phuong",30),("hoang",25)])
#   reduceByKey: perform aggregate before shuffle(reduce before shuffle) down less data than groupbykey
billDebt = rdd.reduceByKey(lambda x,y:x+y)
for key,value in billDebt.collect():
    print(key,value)

# 🔹 How reduceByKey() works:
# Aggregation happens locally within each partition before shuffle.
# If partition 1 has ("a", 1) and ("a", 3), it first sums them up to ("a", 4") locally.
# Only aggregated results are shuffled across the network.
# Instead of sending all occurrences of "a", Spark only sends one final value per partition.
# 🔥 Benefits of reduceByKey(): ✅ Less data is shuffled across the network.
# ✅ Faster execution with better resource utilization.
# ✅ Prevents memory overload (avoids keeping too many values in memory).

