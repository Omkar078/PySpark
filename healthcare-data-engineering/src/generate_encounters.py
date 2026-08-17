import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop_home_dir"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, date_add


spark = (
    SparkSession.builder
    .appName("Healthcare Encounters Data Generator")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Generate 10 million encounters
df = (
    spark.range(1, 10_000_001)
    .withColumnRenamed("id", "encounter_id")
)


# Assign a patient
df = df.withColumn(
    "patient_id",
    expr("cast(floor(rand(10) * 5000000) + 1 as bigint)")
)


# Encounter date
df = df.withColumn(
    "encounter_date",
    date_add(
        lit("2020-01-01"),
        expr("cast(floor(rand(20) * 2400) as int)")
    )
)


# Encounter type
df = df.withColumn(
    "encounter_type",
    expr("""
        element_at(
            array(
                'Outpatient',
                'Inpatient',
                'Emergency',
                'Telehealth',
                'Follow-up'
            ),
            cast(floor(rand(30) * 5) + 1 as int)
        )
    """)
)


# Assign provider
df = df.withColumn(
    "provider_id",
    expr("cast(floor(rand(40) * 100000) + 1 as bigint)")
)


# Assign facility
df = df.withColumn(
    "facility_id",
    expr("cast(floor(rand(50) * 5000) + 1 as bigint)")
)


# Select final columns
df = df.select(
    "encounter_id",
    "patient_id",
    "encounter_date",
    "encounter_type",
    "provider_id",
    "facility_id"
)


# Display sample
print("Encounters Sample:")
df.show(10, truncate=False)


# Display schema
print("Encounters Schema:")
df.printSchema()


# Count records
print("Total Encounters:", df.count())


# Write to Parquet
output_path = "data/raw/encounters"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)


print(f"Encounters data written to: {output_path}")


spark.stop()