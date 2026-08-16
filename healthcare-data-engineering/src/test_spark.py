from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HealthcareDataEngineering") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Version:", spark.version)

spark.stop()