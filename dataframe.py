from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

schema = "Date date, Open float, High float, Low float, Close float, Volume float, Currency string"
df = spark.read.csv('data/coffee.csv', header=True, schema=schema)