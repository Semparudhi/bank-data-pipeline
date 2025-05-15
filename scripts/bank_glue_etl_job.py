import sys
import json
import boto3
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# --- Read config params from arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "CONFIG_S3_PATH", "S3_BUCKET"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --- Load JSON config from S3
config_path = args["CONFIG_S3_PATH"]
bucket = args["S3_BUCKET"]
key = "/".join(config_path.replace("s3://", "").split("/")[1:])

s3 = boto3.client("s3")
config_obj = s3.get_object(Bucket=bucket, Key=key)
params = json.loads(config_obj["Body"].read().decode("utf-8"))
input_path = f"s3://{bucket}/{params['input_path']}"
output_path = f"s3://{bucket}/{params['destination_path']}"


# --- UDF for categorization
def customer_categorization(age, balance):
    if age is None or balance is None:
        return "Unknown"
    if age < 25 and balance < 10000:
        return "Youth Low"
    elif age > 45 and balance > 100000:
        return "Senior High"
    else:
        return "General"


category_udf = F.udf(customer_categorization, StringType())

# --- Read CSV/Parquet from S3
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# --- Filter + format
df = df.filter(
    (F.col("TransactionAmount (INR)") >= 0)
    & (F.col("TransactionAmount (INR)").isNotNull())
)
df = (
    df.withColumn("customer_dob", F.to_date("CustomerDOB", "yyyy-MM-dd"))
    .withColumn("transaction_date", F.to_date("TransactionDate", "yyyy-MM-dd"))
    .withColumn(
        "Age", (F.datediff(F.current_date(), "customer_dob") / 365.25).cast("int")
    )
)

# --- Add segment column
df = df.withColumn("Segment", category_udf(F.col("Age"), F.col("CustAccountBalance")))

# --- Add window functions
windowSpec = Window.partitionBy("CustomerID").orderBy("transaction_date")
df = df.withColumn(
    "days_between_txns",
    F.datediff("transaction_date", F.lag("transaction_date", 1).over(windowSpec)),
).withColumn("total_spent", F.sum("TransactionAmount (INR)").over(windowSpec))

# --- Write output partitioned by location & segment
df.write.mode("overwrite").partitionBy("CustLocation", "Segment").parquet(output_path)

print("Job completed successfully.")
