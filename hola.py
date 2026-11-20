from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HolaSpark").getOrCreate()
sc = spark.sparkContext

numeros = sc.parallelize(range(101))
print("La suma de 0 a 100 es:", numeros.sum())

spark.stop()
