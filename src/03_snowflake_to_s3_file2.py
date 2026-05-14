from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("Snowflake_To_S3_File2").getOrCreate()

FILE2_OUTPUT_PATH = "s3://dijo-emr-project/processed/business_output_file2/"

sfOptions = {
    "sfURL": "your_account.region.aws.snowflakecomputing.com",
    "sfUser": "your_username",
    "sfPassword": "your_password",
    "sfDatabase": "DE_PROJECT_DB",
    "sfSchema": "DE_SCHEMA",
    "sfWarehouse": "COMPUTE_WH"
}

snowflake_df = spark.read \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "RANDOM_USERS_FILE2") \
    .load()

business_df = snowflake_df.withColumn(
    "loan_eligibility",
    when(
        (col("credit_score") >= 600) &
        (col("customer_status") == "active"),
        "Eligible"
    ).otherwise("Not Eligible")
)

business_df.write.mode("overwrite").parquet(FILE2_OUTPUT_PATH)

print("Processed File 2 written successfully to S3")
print("Row count:", business_df.count())
