from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, when

spark = (
    SparkSession.builder
    .appName("AmazonRx_Silver_Reddit_Clean")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.profile.ProfileCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.profile", "rxpulse")  # 👈 profile name
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)

print("🟢 Starting Silver Layer Job")

BRONZE_PATH = "s3a://amzn-rx/bronze/reddit_posts/reddit_posts.json"
SILVER_PATH = "s3a://amzn-rx/silver/reddit_posts_clean/"

# 1. Read Bronze
df = spark.read.option("multiLine", "true").json(BRONZE_PATH)
print("Bronze record count:", df.count())

# 2. Clean text
df_clean = df.withColumn(
    "title_clean",
    lower(trim(col("title")))
)

# 3. Filter Amazon Pharmacy mentions
df_filtered = df_clean.filter(
    col("title_clean").contains("amazon") &
    col("title_clean").contains("pharmacy")
)

# 4. Issue classification
df_enriched = df_filtered.withColumn(
    "issue_type",
    when(col("title_clean").rlike("delay|late|delivery|time"), "DELIVERY_DELAY")
    .when(col("title_clean").rlike("price|cost|expensive|\\$"), "PRICING")
    .when(col("title_clean").rlike("trust|credible|scam|don’t use"), "TRUST_ISSUE")
    .otherwise("GENERAL_FEEDBACK")
)

# 5. Final Silver schema
df_silver = df_enriched.select(
    col("source"),
    col("created_date"),
    col("title_clean").alias("title"),
    col("issue_type")
)

print("Final Silver Preview:")
df_silver.show(10, truncate=False)

# 6. Write Silver
(
    df_silver
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(SILVER_PATH)
)

print("💾 Silver layer written to:", SILVER_PATH)

spark.stop()
print("🛑 Silver job completed successfully")
