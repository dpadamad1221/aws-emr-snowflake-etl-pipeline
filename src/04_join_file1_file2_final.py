from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Join_File1_File2_Final").getOrCreate()

FILE1_INPUT_PATH = "s3://dijo-emr-project/processed/random_users_flattened/"
FILE2_INPUT_PATH = "s3://dijo-emr-project/processed/business_output_file2/"
FINAL_OUTPUT_PATH = "s3://dijo-emr-project/final/final_joined_output/"

file1_df = spark.read.parquet(FILE1_INPUT_PATH)
file2_df = spark.read.parquet(FILE2_INPUT_PATH)

f1 = file1_df.alias("f1")
f2 = file2_df.alias("f2")

final_joined_df = f1.join(
    f2,
    col("f1.uuid") == col("f2.uuid"),
    "inner"
).select(
    col("f1.uuid"),
    col("f1.first_name"),
    col("f1.last_name"),
    col("f1.gender"),
    col("f1.country").alias("user_country"),
    col("f1.email"),
    col("f2.credit_score"),
    col("f2.customer_status"),
    col("f2.risk_category"),
    col("f2.loan_eligibility")
)

final_joined_df.write.mode("overwrite").parquet(FINAL_OUTPUT_PATH)

print("Final joined output written successfully to S3")
print("Final row count:", final_joined_df.count())
