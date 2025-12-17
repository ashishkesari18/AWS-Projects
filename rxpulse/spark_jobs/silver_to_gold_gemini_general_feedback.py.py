import os
import re
import json
from datetime import date
from typing import Dict, Any, List

from dotenv import load_dotenv
import google.generativeai as genai
from tenacity import retry, wait_fixed, stop_after_attempt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
MAX_ROWS = int(os.getenv("GEMINI_MAX_ROWS", "10"))  # keep small for free-tier
S3_BUCKET = os.getenv("S3_BUCKET", "amzn-rx")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

SILVER_PATH = f"s3a://{S3_BUCKET}/silver/reddit_posts_clean/"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RxPulse_Gemini_General_Feedback")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )


def extract_json(text: str) -> Dict[str, Any]:
    if not text or not isinstance(text, str):
        raise ValueError("Empty Gemini response")

    # Remove markdown fences if present
    cleaned = text.strip()
    cleaned = cleaned.replace("```json", "").replace("```", "").strip()

    # Find first {...} block
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if not match:
        raise ValueError(f"No JSON object found in response: {cleaned[:200]}")

    return json.loads(match.group())


@retry(wait=wait_fixed(10), stop=stop_after_attempt(3))
def gemini_enrich(title: str) -> Dict[str, Any]:
    """
    Calls Gemini and returns parsed JSON.
    Retries on transient failures.
    """
    prompt = f"""
You are an analyst reviewing customer feedback for Amazon Pharmacy.

Return ONLY valid JSON (no markdown, no explanation, no extra text).

Schema:
{{
  "sentiment": "Positive | Neutral | Negative",
  "hidden_theme": "short phrase",
  "risk_level": "Low | Medium | High",
  "action": "One-line recommendation"
}}

Post:
"{title}"
""".strip()

    model = genai.GenerativeModel(MODEL_NAME)
    resp = model.generate_content(prompt)
    # Gemini SDK response text can be in resp.text
    return extract_json(resp.text)


def main():
    load_dotenv()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "GEMINI_API_KEY not found. Add it to .env like:\n"
            "GEMINI_API_KEY=xxxxx\n"
        )

    genai.configure(api_key=api_key)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    TODAY = date.today().isoformat()
    GOLD_AI_PATH = f"s3a://{S3_BUCKET}/gold/general_feedback_ai/dt={TODAY}/"

    print("Starting Gemini enrichment (GENERAL_FEEDBACK only)")
    print("Model:", MODEL_NAME)
    print("Max rows:", MAX_ROWS)
    print("Silver path:", SILVER_PATH)
    print("Gold AI path:", GOLD_AI_PATH)

    # --- Read from S3 Silver ---
    df = (
        spark.read.parquet(SILVER_PATH)
        .filter(col("issue_type") == "GENERAL_FEEDBACK")
        .select("title", "created_date", "source", "issue_type")
        .limit(MAX_ROWS)
    )

    rows = df.collect()
    print(f"Rows to enrich: {len(rows)}")

    enriched: List[Dict[str, Any]] = []

    for i, r in enumerate(rows, start=1):
        title = r["title"] or ""
        if not title.strip():
            print(f"Skipping empty title at row {i}")
            continue

        print(f"Enriching {i}/{len(rows)}")
        try:
            ai = gemini_enrich(title)

            # Defensive defaults (in case model returns something weird)
            sentiment = ai.get("sentiment", "Neutral")
            hidden_theme = ai.get("hidden_theme", "unknown")
            risk_level = ai.get("risk_level", "Low")
            action = ai.get("action", "")

            enriched.append({
                "dt": TODAY,
                "issue_type": r["issue_type"],
                "title": title,
                "created_date": r["created_date"],
                "source": r["source"],
                "sentiment": sentiment,
                "hidden_theme": hidden_theme,
                "risk_level": risk_level,
                "action": action
            })

        except Exception as e:
            print(f"Skipped row {i} due to Gemini/parsing error: {str(e)[:180]}")

    if not enriched:
        print("No AI insights produced. Nothing to write.")
        spark.stop()
        return

    # --- Write to S3 Gold ---
    out_df = spark.createDataFrame(enriched)

    (
        out_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(GOLD_AI_PATH)
    )

    print("Gemini AI insights written to:", GOLD_AI_PATH)
    spark.stop()
    print("Job complete")


if __name__ == "__main__":
    main()
