
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName("jsonDF")\
    .master("local[*]")\
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .getOrCreate()

schemaType = StructType([
    StructField("id",StringType(),True),
    StructField("type",StringType(),True),
    StructField("actor",StructType([
        StructField("id",IntegerType(),True),
        StructField("login",StringType(),True),
        StructField("gravatar_id",StringType(),True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True)
    ]),True),
    StructField("repo", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True)
    ]), True),
    StructField("payload", StructType([
        StructField("action",StringType(),True),
        StructField("issue",StructType([
            StructField("url", StringType(), True),
            StructField("labels_url", StringType(), True),
            StructField("comments_url", StringType(), True),
            StructField("events_url", StringType(), True),
            StructField("html_url", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("number", IntegerType(), True),
            StructField("title", StringType(), True),

            StructField("user",StructType([
                StructField("login", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True)
            ]),True),

            StructField("labels",ArrayType(
                StructType([
                StructField("url", StringType(), True),
                StructField("name", StringType(), True),
                StructField("color", StringType(), True)
            ]),True),True),

            StructField("state",StringType(),True),
            StructField("locked", BooleanType(), True),
            StructField("assignee", StringType(), True),
            StructField("milestone", StringType(), True),
            StructField("comments", IntegerType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("closed_at", TimestampType(), True),
            StructField("body", StringType(), True)
        ]),True)
    ]),True),
    StructField("public",BooleanType(),True),
    StructField("created_at", TimestampType(), True),
])

jsonData = spark.read.schema(schemaType).json("../../data/2015-03-01-17.json")
# jsonData2 = spark.read.option('header','true').option('inferSchema','true')\
#     .csv('../../data/2015-03-01-17.json')
# jsonData.printSchema()
# jsonData.select("actor.login", "repo.name", "payload.issue.title").show(10, False)

# jsonData.select(col("id").alias("user_id"),col("payload.action").alias('action')).show()

#select with expression

# jsonData.selectExpr(
#     "count(distinct(id))"
# ).show()
# jsonData.where('actor.id<20000').show()
# jsonData.select(col('id'),col('actor.id').alias('actor_id')).filter(col('id') == '2615567826').show()
# jsonData.select(col('payload.issue.state')).distinct().selectExpr()

# jsonData.select(
#     col("id"),
#     (col("id") < "12345").alias("smaller_id")
# ).show()

jsonData.select(col('payload.issue.state').alias('state')).dropDuplicates().show()