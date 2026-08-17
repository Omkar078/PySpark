import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, date_add, col


spark = (
    SparkSession.builder
    .appName("Healthcare Medications Data Generator")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Generate 15 million medication records
df = (
    spark.range(1, 15_000_001)
    .withColumnRenamed("id", "medication_id")
)


# Assign patient
df = df.withColumn(
    "patient_id",
    expr(
        "cast(floor(rand(10) * 5000000) + 1 as bigint)"
    )
)


# Assign encounter
df = df.withColumn(
    "encounter_id",
    expr(
        "cast(floor(rand(20) * 10000000) + 1 as bigint)"
    )
)


# Medication name
df = df.withColumn(
    "medication_name",
    expr("""
        element_at(
            array(
                'Metformin',
                'Atorvastatin',
                'Lisinopril',
                'Amlodipine',
                'Losartan',
                'Omeprazole',
                'Levothyroxine',
                'Aspirin',
                'Ibuprofen',
                'Amoxicillin',
                'Azithromycin',
                'Paracetamol',
                'Gabapentin',
                'Sertraline',
                'Insulin'
            ),
            cast(floor(rand(30) * 15) + 1 as int)
        )
    """)
)


# Medication dose
df = df.withColumn(
    "dose",
    expr("""
        cast(
            element_at(
                array(
                    5.0,
                    10.0,
                    20.0,
                    25.0,
                    50.0,
                    75.0,
                    100.0,
                    250.0,
                    500.0
                ),
                cast(floor(rand(40) * 9) + 1 as int)
            )
            as double
        )
    """)
)



# Start date
df = df.withColumn(
    "start_date",
    date_add(
        lit("2020-01-01"),
        expr(
            "cast(floor(rand(50) * 2400) as int)"
        )
    )
)


# End date
df = df.withColumn(
    "end_date",
    date_add(
        col("start_date"),
        expr(
            "cast(floor(rand(60) * 365) + 30 as int)"
        )
    )
)


# Select final columns according to project schema
df = df.select(
    "medication_id",
    "patient_id",
    "encounter_id",
    "medication_name",
    "dose",
    "start_date",
    "end_date"
)


# Display sample
print("Medications Sample:")
df.show(10, truncate=False)


# Display schema
print("Medications Schema:")
df.printSchema()


# Count records
print("Total Medications:", df.count())


# Write to Parquet
output_path = "data/raw/medications"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)


print(f"Medications data written to: {output_path}")


spark.stop()