from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "API_To_S3_File1"
).getOrCreate()

# Read Random User API JSON data
df = spark.read.json(
    "s3://dijo-emr-project/raw/random_user.json"
)

# Placeholder for flattening logic

print("File 1 pipeline completed")
