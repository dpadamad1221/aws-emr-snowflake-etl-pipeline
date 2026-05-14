from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("API_To_S3_File1").getOrCreate()

RAW_JSON_PATH = "s3://dijo-emr-project/raw/random_user.json"
FILE1_OUTPUT_PATH = "s3://dijo-emr-project/processed/random_users_flattened/"

df = spark.read.json(RAW_JSON_PATH)

final_df = df.select(
    col("user.gender").alias("gender"),
    col("user.name.title").alias("title"),
    col("user.name.first").alias("first_name"),
    col("user.name.last").alias("last_name"),
    col("user.location.city").alias("city"),
    col("user.location.state").alias("state"),
    col("user.location.country").alias("country"),
    col("user.location.postcode").alias("postcode"),
    col("user.location.street.name").alias("street_name"),
    col("user.location.street.number").alias("street_number"),
    col("user.location.coordinates.latitude").alias("latitude"),
    col("user.location.coordinates.longitude").alias("longitude"),
    col("user.location.timezone.offset").alias("timezone_offset"),
    col("user.location.timezone.description").alias("timezone_description"),
    col("user.email").alias("email"),
    col("user.login.username").alias("username"),
    col("user.login.uuid").alias("uuid"),
    col("user.dob.age").alias("dob_age"),
    col("user.dob.date").alias("dob_date"),
    col("user.registered.age").alias("registered_age"),
    col("user.registered.date").alias("registered_date"),
    col("user.phone").alias("phone"),
    col("user.cell").alias("cell"),
    col("user.nat").alias("nationality")
)

final_df.write.mode("overwrite").parquet(FILE1_OUTPUT_PATH)

print("File 1 written successfully to S3")
print("Row count:", final_df.count())
