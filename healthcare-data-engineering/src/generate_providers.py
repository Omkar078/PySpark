import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop_home_dir"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, concat, lit


spark = (
    SparkSession.builder
    .appName("Healthcare Providers Data Generator")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Generate 100,000 providers
df = (
    spark.range(1, 100_001)
    .withColumnRenamed("id", "provider_id")
)


# Provider name
df = df.withColumn(
    "provider_name",
    concat(
        expr("""
            element_at(
                array(
                    'Aarav','Rahul','Amit','Rohan','Vikas',
                    'Karan','Raj','Arjun','Aditya','Vivek',
                    'Priya','Pooja','Sneha','Neha','Aisha',
                    'Riya','Isha','Kavya','Ananya','Megha'
                ),
                cast(floor(rand(10) * 20) + 1 as int)
            )
        """),
        lit(" "),
        expr("""
            element_at(
                array(
                    'Sharma','Patel','Panchal','Mehta','Joshi',
                    'Singh','Verma','Gupta','Reddy','Nair',
                    'Iyer','Shah','Kapoor','Jain','Khan'
                ),
                cast(floor(rand(20) * 15) + 1 as int)
            )
        """)
    )
)


# Medical specialty
df = df.withColumn(
    "specialty",
    expr("""
        element_at(
            array(
                'Cardiology',
                'Neurology',
                'Oncology',
                'Orthopedics',
                'Pediatrics',
                'Dermatology',
                'Psychiatry',
                'General Medicine',
                'Gynecology',
                'Gastroenterology',
                'Endocrinology',
                'Pulmonology',
                'Urology',
                'Ophthalmology',
                'Radiology'
            ),
            cast(floor(rand(30) * 15) + 1 as int)
        )
    """)
)


# Assign provider to one of the 5,000 facilities
df = df.withColumn(
    "facility_id",
    expr("cast(floor(rand(40) * 5000) + 1 as bigint)")
)


# Select final columns
df = df.select(
    "provider_id",
    "provider_name",
    "specialty",
    "facility_id"
)


# Display sample
print("Providers Sample:")
df.show(10, truncate=False)


# Display schema
print("Providers Schema:")
df.printSchema()


# Count records
print("Total Providers:", df.count())


# Write to Parquet
output_path = "data/raw/providers"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)


print(f"Providers data written to: {output_path}")


spark.stop()