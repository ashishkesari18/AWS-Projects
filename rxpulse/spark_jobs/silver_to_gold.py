from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    approx_count_distinct,
    lit
)
from datetime import date


def main():
    print("🟡 Starting Gold Layer Job")

    spark = (
        SparkSession.builder
        .appName("AmazonRx_Gold_Aggregations")

        # ---------------- S3 SUPPORT ----------------
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        # ------------------------------------------------

        .getOrCreate()
    )

    # ---------- Paths ----------
    SILVER_PATH = "s3a://amzn-rx/silver/reddit_posts_clean/"
    TODAY_DT = date.today().isoformat()

    GOLD_FACT_DAILY = f"s3a://amzn-rx/gold/fact_issue_daily/dt={TODAY_DT}/"
    GOLD_TOP_POSTS = f"s3a://amzn-rx/gold/top_issue_posts/dt={TODAY_DT}/"

    # ---------- Read Silver ----------
    silver_df = spark.read.parquet(SILVER_PATH)

    print("Silver record count:", silver_df.count())

    # ---------- Gold Table 1: Daily Issue Aggregates ----------
    fact_issue_daily = (
        silver_df
        .groupBy("issue_type")
        .agg(
            count("*").alias("mentions"),
            approx_count_distinct("title").alias("unique_titles")
        )
        .withColumn("dt", lit(TODAY_DT))
        .select("dt", "issue_type", "mentions", "unique_titles")
    )

    print("Writing Gold: fact_issue_daily")

    (
        fact_issue_daily
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(GOLD_FACT_DAILY)
    )

    # ---------- Gold Table 2: Top Issue Posts ----------
    top_issue_posts = (
        silver_df
        .select(
            lit(TODAY_DT).alias("dt"),
            col("issue_type"),
            col("title"),
            col("created_date"),
            col("source")
        )
    )

    print("Writing Gold: top_issue_posts")

    (
        top_issue_posts
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(GOLD_TOP_POSTS)
    )

    print("✅ Gold layer written successfully")
    print("📍 fact_issue_daily:", GOLD_FACT_DAILY)
    print("📍 top_issue_posts:", GOLD_TOP_POSTS)

    spark.stop()
    print("🛑 Gold job completed")


if __name__ == "__main__":
    main()
