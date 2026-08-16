import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop_home_dir"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

print("HADOOP_HOME:", os.environ["HADOOP_HOME"])
print("winutils exists:", os.path.exists(r"C:\hadoop\bin\winutils.exe"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    lit,
    when
)


# Create Spark Session
spark = SparkSession.builder \
    .appName("GenerateFacilities") \
    .master("local[*]") \
    .getOrCreate()


# Generate 5,000 facility IDs
df = spark.range(1, 5001) \
    .withColumnRenamed("id", "facility_id")


# Generate facility names
df = df.withColumn(
    "facility_name",
    concat(
        lit("Healthcare Facility "),
        col("facility_id")
    )
)


# Generate cities
df = df.withColumn(
    "city",
    when((col("facility_id") % 10) == 0, "Mumbai")
    .when((col("facility_id") % 10) == 1, "Pune")
    .when((col("facility_id") % 10) == 2, "Delhi")
    .when((col("facility_id") % 10) == 3, "Bangalore")
    .when((col("facility_id") % 10) == 4, "Hyderabad")
    .when((col("facility_id") % 10) == 5, "Chennai")
    .when((col("facility_id") % 10) == 6, "Kolkata")
    .when((col("facility_id") % 10) == 7, "Ahmedabad")
    .when((col("facility_id") % 10) == 8, "Jaipur")
    .otherwise("Nagpur")
)


# Generate states
df = df.withColumn(
    "state",
    when(col("city") == "Mumbai", "Maharashtra")
    .when(col("city") == "Pune", "Maharashtra")
    .when(col("city") == "Delhi", "Delhi")
    .when(col("city") == "Bangalore", "Karnataka")
    .when(col("city") == "Hyderabad", "Telangana")
    .when(col("city") == "Chennai", "Tamil Nadu")
    .when(col("city") == "Kolkata", "West Bengal")
    .when(col("city") == "Ahmedabad", "Gujarat")
    .when(col("city") == "Jaipur", "Rajasthan")
    .otherwise("Maharashtra")
)


# Generate facility types
df = df.withColumn(
    "facility_type",
    when((col("facility_id") % 4) == 0, "Hospital")
    .when((col("facility_id") % 4) == 1, "Clinic")
    .when((col("facility_id") % 4) == 2, "Diagnostic Center")
    .otherwise("Specialty Center")
)


# Select columns
df = df.select(
    "facility_id",
    "facility_name",
    "city",
    "state",
    "facility_type"
)


# Display sample records
df.show(10, truncate=False)


# Display schema
df.printSchema()


# Display record count
print("Total Facilities:", df.count())


# Write data as Parquet
output_path = r"C:\Users\omkar\OneDrive\Documents\GitHub_Personal\PySpark\healthcare-data-engineering\data\raw\facilities"

df.write \
    .mode("overwrite") \
    .parquet(output_path)


# Stop Spark
spark.stop()