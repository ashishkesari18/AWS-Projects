from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
print("SPARK STARTED SUCCESSFULLY")

data = [("Amazon Pharmacy Delay",), ("Hello World",)]
df = spark.createDataFrame(data, ["title"])

print("ROW COUNT:", df.count())
df.show()

spark.stop()
