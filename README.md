# AWS EMR + Snowflake ETL Pipeline

## Project Overview

This project demonstrates an end-to-end Data Engineering pipeline using:

- AWS EMR
- PySpark
- Amazon S3
- Snowflake
- Spark SQL
- Git & GitHub workflow

The pipeline ingests nested JSON data from the Random User API, performs flattening and transformations using PySpark, writes curated Parquet datasets to Amazon S3, integrates Snowflake as a secondary source system, applies business logic, performs multi-source joins, and stores the final analytical dataset back into S3.

---

## Architecture

API JSON Data
↓
PySpark Flattening
↓
S3 Parquet (File 1)
↓
Business Dataset Generation
↓
Snowflake Integration
↓
PySpark Business Logic
↓
File 2 Written to S3
↓
Multi-source Join
↓
Final Curated Dataset to S3

---

## Technologies Used

- Python
- PySpark
- AWS EMR
- Amazon S3
- Snowflake
- Spark SQL
- GitHub Actions
- Git

---

## Key Features

- Nested JSON flattening
- Parquet optimization
- Snowflake-Spark integration
- Multi-source ETL pipeline
- Business rule transformations
- UUID-based joins
- Git branching workflow
- CI pipeline using GitHub Actions

---

## Dataset

Source API:
https://randomuser.me/api/

---

## Sample Business Logic

- Credit score generation
- Customer status classification
- Loan eligibility derivation

---

## Final Output Schema

- uuid
- first_name
- last_name
- gender
- user_country
- email
- credit_score
- customer_status
- risk_category
- loan_eligibility

---

## Future Enhancements

- Apache Airflow orchestration
- Incremental data loading
- Athena integration
- Snowflake staging optimization
- CI/CD deployment pipeline
- Elasticsearch integration

