from operator import truediv
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

schema = "Date string, Open float, High float, Low float, Close float, Volume float, Currency string"
df = spark.read.csv('data/coffee.csv', header=True, schema=schema)