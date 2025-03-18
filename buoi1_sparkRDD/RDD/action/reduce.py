from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ObjectTransformation") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .master("local[*]")\
    .getOrCreate()

sc = spark.sparkContext
numbers = sc.parallelize([1,2,3,4,5,6,7,8,9,10,11,12],numSlices=3)
# numbers = numbers.reduce(lambda x,y: x + y)
# print(numbers)

def tinhtong(x:int,y:int)->int:
    print(f"{x}+{y}=>{x+y}")
    return x + y
numbers = numbers.reduce(lambda x,y: x + y)
print(numbers)

#sẽ thực hiện reduce() trên từng partition trước, sau đó mới gộp kết quả từ các partition lại với nhau.
#