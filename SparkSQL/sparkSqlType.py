
import random
from datetime import datetime

from pyspark import SparkConf, SparkContext, Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("KeyValue") \
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .config("spark.executor.memory","4g")\
    .getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([
    Row(
        name = "Duc Anh",
        age = 18,
        id = 1,
        salary = 3000.0,
        bonus = 1000.5,
        is_active=True,
        scores=[1,8,10],
        attributes={"dep":"massage","role":"De"},
        hire_date = datetime.strptime("2025-01-10","%Y-%m-%d").date(),
        last_login = datetime.strptime("2025-03-16","%Y-%m-%d").date(),
    ),
    Row(
        name = "Phuong",
        age = 22,
        id = 2,
        salary = 2000.0,
        bonus = 3000.5,
        is_active=False,
        scores=[2,3,10],
        attributes={"dep":"tea","role":"tester"},
        hire_date = datetime.strptime("2025-02-12","%Y-%m-%d").date(),
        last_login = datetime.strptime("2025-04-17","%Y-%m-%d").date(),
    )
])

schemaPeople =  StructType([
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=False),
    StructField("id", IntegerType(), nullable=False),
    StructField("salary", DoubleType(), nullable=False),
    StructField("bonus", DoubleType(), nullable=False),
    StructField("is_active", BooleanType(), nullable=False),
    StructField("scores", ArrayType(IntegerType()), nullable=False),
    StructField("attributes", MapType(StringType(), StringType()), nullable=False),
    StructField("hire_date", DateType(), nullable=False),
    StructField("last_login", DateType(), nullable=False)
])

df = spark.createDataFrame(data=rdd, schema=schemaPeople)
df.show(truncate=False)
df.printSchema()