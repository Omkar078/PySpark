import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop_home_dir"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, date_add


spark = (
    SparkSession.builder
    .appName("Healthcare Patients Data Generator")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Generate 5 million patients
df = (
    spark.range(1, 5_000_001)
    .withColumnRenamed("id", "patient_id")
)


# First name
df = df.withColumn(
    "first_name",
    expr("""
        element_at(
            array(
                'Aarav','Vivaan','Aditya','Arjun','Rahul',
                'Rohan','Amit','Vikas','Karan','Raj',
                'Ananya','Priya','Pooja','Sneha','Neha',
                'Aisha','Riya','Isha','Kavya','Diya'
            ),
            cast(floor(rand(10) * 20) + 1 as int)
        )
    """)
)


# Last name
df = df.withColumn(
    "last_name",
    expr("""
        element_at(
            array(
                'Sharma','Patel','Panchal','Deshmukh','Mehta',
                'Joshi','Kulkarni','Singh','Verma','Gupta',
                'Reddy','Nair','Iyer','Shah','Kapoor',
                'Malhotra','Chopra','Mishra','Jain','Khan'
            ),
            cast(floor(rand(20) * 20) + 1 as int)
        )
    """)
)


# Gender
df = df.withColumn(
    "gender",
    expr("""
        element_at(
            array('Male', 'Female', 'Other'),
            cast(floor(rand(30) * 3) + 1 as int)
        )
    """)
)


# Date of birth
df = df.withColumn(
    "date_of_birth",
    date_add(
        lit("1960-01-01"),
        expr("cast(floor(rand(40) * 20000) as int)")
    )
)


# City
df = df.withColumn(
    "city",
    expr("""
        element_at(
            array(
                'Mumbai','Pune','Delhi','Bangalore',
                'Hyderabad','Chennai','Kolkata',
                'Ahmedabad','Jaipur','Surat'
            ),
            cast(floor(rand(50) * 10) + 1 as int)
        )
    """)
)


# State
df = df.withColumn(
    "state",
    expr("""
        CASE
            WHEN city IN ('Mumbai', 'Pune') THEN 'Maharashtra'
            WHEN city = 'Delhi' THEN 'Delhi'
            WHEN city = 'Bangalore' THEN 'Karnataka'
            WHEN city = 'Hyderabad' THEN 'Telangana'
            WHEN city = 'Chennai' THEN 'Tamil Nadu'
            WHEN city = 'Kolkata' THEN 'West Bengal'
            WHEN city = 'Ahmedabad' THEN 'Gujarat'
            WHEN city = 'Jaipur' THEN 'Rajasthan'
            WHEN city = 'Surat' THEN 'Gujarat'
        END
    """)
)


# ZIP code
df = df.withColumn(
    "zip_code",
    expr("cast(floor(rand(60) * 900000) + 100000 as string)")
)


# Registration date
df = df.withColumn(
    "registration_date",
    date_add(
        lit("2020-01-01"),
        expr("cast(floor(rand(70) * 2400) as int)")
    )
)


# Select columns according to project schema
df = df.select(
    "patient_id",
    "first_name",
    "last_name",
    "gender",
    "date_of_birth",
    "city",
    "state",
    "zip_code",
    "registration_date"
)


# Display sample
print("Patients Sample:")
df.show(10, truncate=False)


# Display schema
print("Patients Schema:")
df.printSchema()


# Count records
print("Total Patients:", df.count())


# Write to Parquet
output_path = "data/raw/patients"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)

print(f"Patients data written to: {output_path}")


spark.stop()