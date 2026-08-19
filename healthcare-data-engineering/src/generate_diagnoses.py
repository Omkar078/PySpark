import os

# Configure Hadoop environment for Windows.
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop_home_dir"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, date_add


# ---------------------------------------------------------
# Create Spark Session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Healthcare Diagnoses Data Generator")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# Read existing Encounters
# ---------------------------------------------------------

encounters_path = "data/raw/encounters"

encounters_df = (
    spark.read
    .parquet(encounters_path)
    .select(
        "encounter_id",
        "patient_id",
        "encounter_date"
    )
)


# ---------------------------------------------------------
# Generate 15 million diagnoses
# ---------------------------------------------------------

df = (
    spark.range(1, 15_000_001)
    .withColumnRenamed("id", "diagnosis_id")
)


# ---------------------------------------------------------
# Assign an existing encounter
# ---------------------------------------------------------

df = df.withColumn(
    "encounter_index",
    expr(
        "cast(floor(rand(10) * 10000000) + 1 as bigint)"
    )
)


# Use the encounter index to obtain a valid encounter_id.
encounters_indexed = (
    encounters_df
    .withColumn(
        "encounter_index",
        expr(
            "row_number() over (order by encounter_id)"
        )
    )
)


# Join diagnosis records with encounters.
df = (
    df.join(
        encounters_indexed,
        on="encounter_index",
        how="left"
    )
)


# ---------------------------------------------------------
# Assign patient from the associated encounter
# ---------------------------------------------------------

df = df.withColumn(
    "patient_id",
    col("patient_id")
)


# ---------------------------------------------------------
# Diagnosis code
# ---------------------------------------------------------

df = df.withColumn(
    "diagnosis_code",
    expr("""
        element_at(
            array(
                'I10',
                'E11.9',
                'E78.5',
                'J45.909',
                'J44.9',
                'C50.919',
                'C34.90',
                'M54.5',
                'K21.9',
                'F32.9',
                'G43.909',
                'N39.0',
                'I25.10',
                'E03.9',
                'D64.9'
            ),
            cast(floor(rand(30) * 15) + 1 as int)
        )
    """)
)


# ---------------------------------------------------------
# Diagnosis description
# ---------------------------------------------------------

df = df.withColumn(
    "diagnosis_description",
    expr("""
        CASE diagnosis_code
            WHEN 'I10' THEN 'Essential hypertension'
            WHEN 'E11.9' THEN 'Type 2 diabetes mellitus'
            WHEN 'E78.5' THEN 'Hyperlipidemia'
            WHEN 'J45.909' THEN 'Asthma'
            WHEN 'J44.9' THEN 'Chronic obstructive pulmonary disease'
            WHEN 'C50.919' THEN 'Malignant neoplasm of breast'
            WHEN 'C34.90' THEN 'Malignant neoplasm of lung'
            WHEN 'M54.5' THEN 'Low back pain'
            WHEN 'K21.9' THEN 'Gastroesophageal reflux disease'
            WHEN 'F32.9' THEN 'Depressive disorder'
            WHEN 'G43.909' THEN 'Migraine'
            WHEN 'N39.0' THEN 'Urinary tract infection'
            WHEN 'I25.10' THEN 'Atherosclerotic heart disease'
            WHEN 'E03.9' THEN 'Hypothyroidism'
            WHEN 'D64.9' THEN 'Anemia'
        END
    """)
)


# ---------------------------------------------------------
# Diagnosis date
# ---------------------------------------------------------

# Generate diagnosis date on the encounter date
# or up to 30 days after the encounter.
df = df.withColumn(
    "diagnosis_date",
    date_add(
        col("encounter_date"),
        expr("cast(floor(rand(40) * 31) as int)")
    )
)


# ---------------------------------------------------------
# Select final columns
# ---------------------------------------------------------

df = df.select(
    "diagnosis_id",
    "encounter_id",
    "patient_id",
    "diagnosis_code",
    "diagnosis_description",
    "diagnosis_date"
)


# ---------------------------------------------------------
# Display sample
# ---------------------------------------------------------

print("Diagnoses Sample:")
df.show(10, truncate=False)


# ---------------------------------------------------------
# Display schema
# ---------------------------------------------------------

print("Diagnoses Schema:")
df.printSchema()


# ---------------------------------------------------------
# Count records
# ---------------------------------------------------------

print("Total Diagnoses:", df.count())


# ---------------------------------------------------------
# Write to Parquet
# ---------------------------------------------------------

output_path = "data/raw/diagnoses"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)


print(f"Diagnoses data written to: {output_path}")


# ---------------------------------------------------------
# Stop Spark
# ---------------------------------------------------------

spark.stop()