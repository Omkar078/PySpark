# Healthcare Data Engineering Pipeline

An end-to-end **Big Data Healthcare Data Engineering project** built using **PySpark, GCP, BigQuery, Parquet, and Apache Airflow**.

## 📌 Project Overview

This project simulates a real-world healthcare data platform that processes **100M+ synthetic EHR records** from multiple sources and transforms them into clean, standardized, analytics-ready datasets.

The pipeline covers data ingestion, quality checks, transformation, clinical cohort creation, and OMOP CDM-aligned data processing.

## 🏗️ Pipeline

```text
Synthetic EHR Data
        ↓
PySpark Ingestion
        ↓
Data Quality & Validation
        ↓
Cleaning & Standardization
        ↓
Joins & Transformations
        ↓
Aggregations & Window Functions
        ↓
Clinical Cohort Creation
        ↓
OMOP CDM Transformation
        ↓
Partitioned Parquet
        ↓
Google Cloud Storage
        ↓
BigQuery
        ↓
Analytics
