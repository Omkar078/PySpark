import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

from pyspark.sql import SparkSession

from schemas import (
    patient_schema,
    encounter_schema,
    provider_schema,
    facility_schema,
    diagnosis_schema,
    medication_schema
)


spark = (
    SparkSession.builder
    .appName("Healthcare Schema Validation")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Expected schema and Parquet location
tables = {
    "Patients": (
        "data/raw/patients",
        patient_schema
    ),
    "Encounters": (
        "data/raw/encounters",
        encounter_schema
    ),
    "Providers": (
        "data/raw/providers",
        provider_schema
    ),
    "Facilities": (
        "data/raw/facilities",
        facility_schema
    ),
    "Diagnoses": (
        "data/raw/diagnoses",
        diagnosis_schema
    ),
    "Medications": (
        "data/raw/medications",
        medication_schema
    )
}


def schemas_match(expected_schema, actual_schema):
    """
    Compare column names, column order and data types.
    Nullability is intentionally ignored.
    """

    expected_fields = [
        (field.name, field.dataType.simpleString())
        for field in expected_schema.fields
    ]

    actual_fields = [
        (field.name, field.dataType.simpleString())
        for field in actual_schema.fields
    ]

    return expected_fields == actual_fields


def validate_schema(table_name, path, expected_schema):

    print("\n" + "=" * 60)
    print(f"Validating: {table_name}")
    print("=" * 60)

    # Check whether Parquet dataset exists
    if not os.path.exists(path):

        print("STATUS: NOT FOUND")
        print(f"Path: {path}")

        return False

    try:

        # Read actual Parquet data
        df = spark.read.parquet(path)

        actual_schema = df.schema

        print("\nExpected Schema:")
        print(expected_schema.simpleString())

        print("\nActual Schema:")
        print(actual_schema.simpleString())


        # Compare schemas
        if schemas_match(expected_schema, actual_schema):

            print("\nSTATUS: PASS")

            return True


        print("\nSTATUS: FAIL")

        expected_fields = {
            field.name: field.dataType.simpleString()
            for field in expected_schema.fields
        }

        actual_fields = {
            field.name: field.dataType.simpleString()
            for field in actual_schema.fields
        }


        # Check missing columns
        missing_columns = (
            set(expected_fields) - set(actual_fields)
        )

        if missing_columns:

            print("\nMissing columns:")

            for column in sorted(missing_columns):
                print(f"  - {column}")


        # Check unexpected columns
        extra_columns = (
            set(actual_fields) - set(expected_fields)
        )

        if extra_columns:

            print("\nUnexpected columns:")

            for column in sorted(extra_columns):
                print(f"  - {column}")


        # Check data type mismatches
        common_columns = (
            set(expected_fields) & set(actual_fields)
        )

        for column in sorted(common_columns):

            expected_type = expected_fields[column]
            actual_type = actual_fields[column]

            if expected_type != actual_type:

                print(
                    f"\nColumn '{column}' type mismatch:"
                )

                print(
                    f"  Expected: {expected_type}"
                )

                print(
                    f"  Actual:   {actual_type}"
                )


        # Check column order
        expected_order = list(expected_fields.keys())
        actual_order = list(actual_fields.keys())

        if expected_order != actual_order:

            print("\nColumn order mismatch:")

            print(
                f"  Expected: {expected_order}"
            )

            print(
                f"  Actual:   {actual_order}"
            )


        return False


    except Exception as e:

        print("\nSTATUS: ERROR")
        print(f"Error: {e}")

        return False


# Track validation results
results = {}


# Validate all tables
for table_name, (path, expected_schema) in tables.items():

    results[table_name] = validate_schema(
        table_name,
        path,
        expected_schema
    )


# Final summary
print("\n" + "=" * 60)
print("SCHEMA VALIDATION SUMMARY")
print("=" * 60)

for table_name, status in results.items():

    if status is True:

        print(f"{table_name:<15} PASS")

    elif status is False:

        path = tables[table_name][0]

        if os.path.exists(path):
            print(f"{table_name:<15} FAIL")
        else:
            print(f"{table_name:<15} NOT FOUND")


print("=" * 60)
print("Schema validation completed.")
print("=" * 60)


spark.stop()