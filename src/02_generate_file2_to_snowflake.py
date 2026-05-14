from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, when

spark = SparkSession.builder.appName("Generate_File2_To_Snowflake").getOrCreate()

FILE1_INPUT_PATH = "s3://dijo-emr-project/processed/random_users_flattened/"

# Snowflake options should be supplied securely in real projects
sfOptions = {
    "sfURL": "your_account.region.aws.snowflakecomputing.com",
    "sfUser": "your_username",
    "sfPassword": "your_password",
    "sfDatabase": "DE_PROJECT_DB",
    "sfSchema": "DE_SCHEMA",
    "sfWarehouse": "COMPUTE_WH"
}

file1_df = spark.read.parquet(FILE1_INPUT_PATH)

file2_df = file1_df.select(
    col("uuid"),
    col("username"),
    col("country"),
    col("dob_age")
).withColumn(
    "credit_score",
    (rand() * 500 + 300).cast("int")
).withColumn(
    "customer_status",
    when(rand() > 0.5, lit("active")).otherwise(lit("inactive"))
).withColumn(
    "risk_category",
    when(col("credit_score") >= 750, lit("low"))
    .when(col("credit_score") >= 600, lit("medium"))
    .otherwise(lit("high"))
)

file2_df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "RANDOM_USERS_FILE2") \
    .mode("overwrite") \
    .save()

print("File 2 loaded successfully into Snowflake")
print("Row count:", file2_df.count())
