# AWS EMR + Snowflake + Airflow ETL Architecture

## End-to-End Workflow

```text
Random User API
        ↓
Amazon S3 Raw Layer
        ↓
AWS EMR Cluster
        ↓
PySpark Transformations
        ↓
Parquet File 1
        ↓
Snowflake Business Layer
        ↓
Processed File 2
        ↓
Final Join Transformation
        ↓
Curated Final Output
        ↓
Amazon S3 Final Layer
```

---

# Airflow Orchestration Workflow

```text
Create EMR Cluster
        ↓
Run ETL Step 1
        ↓
Run ETL Step 2
        ↓
Run ETL Step 3
        ↓
Run Final Join Step
        ↓
Monitor EMR Steps
        ↓
Terminate EMR Cluster
```

---

# Components Used

- Apache Airflow
- AWS EMR
- PySpark
- Amazon S3
- Snowflake
- GitHub Actions
- Python
- Spark SQL

---

# Data Engineering Concepts Demonstrated

- Distributed Processing
- ETL Pipeline Design
- Workflow Orchestration
- Cloud Data Lake Architecture
- Schema Flattening
- Parquet Optimization
- Batch Data Processing
- Cluster Lifecycle Automation
