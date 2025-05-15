from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import json


def customer_categorization(Age: int, Balance: float) -> str:
    if Age < 25 and Balance < 10000:
        return "Youth Low"
    if Age > 45 and Balance > 100000:
        return "Senior High"
    else:
        return "General"


vars = json.load("./configs/pyspark_config.json")

spark = (
    SparkSession().Builder().appName("Bank Transaction").master("yarn").getOrCreate()
)

df = spark.read.parquet(vars["file_path"])

category_udf = F.udf(customer_categorization, StringType)
winowSpec = Window.partitionBy("CustomerID").orderBy("TransactionDate")

filtereddf = df.filter(df.TransactionAmount >= 0 and df.TransactionAmount.isNotNull)

refined_df = filtereddf.withColumn(
    "customer_dob", F.col("CustomerDOB").cast(dataType="Date")
).withColumn("transaction_date", F.col("TransactionDate").cast(dataType="Date"))

refined_df = refined_df.withColumn(
    "Age", F.date_diff(F.curdate(), F.col("customer_dob")) // 365.25
)


categorized_df = refined_df.withColumn(
    "Segment", category_udf(F.col("Age"), F.col("CustomerBalance"))
)

categorized_df.withColumn(
    "transaction_frequency", F.lag("TransactionDate").over(winowSpec)
).withColumn("total_spent", F.sum("TransactionAmount").over(winowSpec))
categorized_df.write.parquet(
    vars["destination_path"], "overwrite", partitionBy=["CustLocation", "Segment"]
)
