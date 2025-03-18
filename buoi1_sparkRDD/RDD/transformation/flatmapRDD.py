from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ObjectTransformation") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .master("local[*]")\
    .getOrCreate()

sc = spark.sparkContext
fileRdd = sc.textFile("text.txt")
worldRDD = fileRdd.map(lambda line:line.split())
print(worldRDD.collect())
