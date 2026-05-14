# AWS EMR + Snowflake ETL Pipeline

## Project Overview

This project demonstrates an end-to-end Data Engineering pipeline built using AWS EMR, PySpark, Amazon S3, Snowflake, Spark SQL, Git, and GitHub Actions.

The pipeline ingests nested JSON data from the Random User API, performs flattening and transformations using PySpark on AWS EMR, stores curated Parquet datasets in Amazon S3, integrates Snowflake as a secondary source system, applies business logic transformations, performs multi-source joins, and writes the final analytical output back to S3.

---

## End-to-End Pipeline Flow

Random User API
↓
AWS EMR PySpark Processing
↓
Nested JSON Flattening
↓
Curated Parquet Output to S3 (File 1)
↓
Business Dataset Generation
↓
Load Business Data into Snowflake
↓
Read Snowflake Data into Spark
↓
Apply Business Logic Transformations
↓
Write Processed Dataset to S3 (File 2)
↓
Join File 1 and File 2 using UUID
↓
Final Curated Output written to S3

---

## Technologies Used

- Python
- PySpark
- AWS EMR
- Amazon S3
- Snowflake
- Spark SQL
- Git
- GitHub Actions
- Linux / Ubuntu

---

## Key Features

- Nested JSON flattening using PySpark
- Multi-source ETL pipeline
- Snowflake integration with Spark
- Business rule transformations
- UUID-based dataset joins
- Parquet-based optimized storage
- AWS cloud data lake architecture
- Git branching workflow
- GitHub Actions CI integration

---

## Business Logic Implemented

The secondary business dataset includes:

- Credit score generation
- Customer status classification
- Risk category derivation
- Loan eligibility calculation

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

## Pipeline Validation

### Final Output Validation

![Final Output](screenshots/Final_output_validation1.png)

---

## Future Enhancements

- Apache Airflow orchestration
- Incremental ETL processing
- Athena integration
- Snowflake staging optimization
- CI/CD deployment pipeline
- Elasticsearch integration
- Data quality checks and monitoring

---

## Author

Dijo Padamadan
