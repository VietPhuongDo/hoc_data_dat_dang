from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("ReduceKeyValue") \
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .getOrCreate()
sc = spark.sparkContext

rdd1 = sc.parallelize([(130,50.5),(120,12.2),(150,50.0),(150,27.6),(160,28.9),(130,20.2)])

rdd2 = sc.parallelize([(130,"a"),(120,"b"),(150,"c"),(150,"d"),(160,"a"),(110,"e")])
joinedRdd = rdd1.join(rdd2).sortByKey(ascending=False)

for result in joinedRdd.collect():
    print(result)

