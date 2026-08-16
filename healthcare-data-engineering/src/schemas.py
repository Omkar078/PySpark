from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    StringType,
    DateType,
    DoubleType
)


# Patients
patient_schema = StructType([
    StructField("patient_id", LongType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("registration_date", DateType(), True)
])


# Encounters
encounter_schema = StructType([
    StructField("encounter_id", LongType(), False),
    StructField("patient_id", LongType(), False),
    StructField("encounter_date", DateType(), True),
    StructField("encounter_type", StringType(), True),
    StructField("provider_id", LongType(), True),
    StructField("facility_id", LongType(), True)
])


# Providers
provider_schema = StructType([
    StructField("provider_id", LongType(), False),
    StructField("provider_name", StringType(), True),
    StructField("specialty", StringType(), True),
    StructField("facility_id", LongType(), True)
])


# Facilities
facility_schema = StructType([
    StructField("facility_id", LongType(), False),
    StructField("facility_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("facility_type", StringType(), True)
])


# Diagnoses
diagnosis_schema = StructType([
    StructField("diagnosis_id", LongType(), False),
    StructField("encounter_id", LongType(), True),
    StructField("patient_id", LongType(), True),
    StructField("diagnosis_code", StringType(), True),
    StructField("diagnosis_description", StringType(), True),
    StructField("diagnosis_date", DateType(), True)
])


# Medications
medication_schema = StructType([
    StructField("medication_id", LongType(), False),
    StructField("patient_id", LongType(), True),
    StructField("encounter_id", LongType(), True),
    StructField("medication_name", StringType(), True),
    StructField("dose", DoubleType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True)
])