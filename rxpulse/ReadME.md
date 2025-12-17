RxPulse – Amazon Pharmacy Voice of Customer Intelligence Platform

Author: Ashish Kesari

RxPulse is an end-to-end data engineering and AI-driven analytics platform designed to capture, process, and surface customer voice signals for Amazon Pharmacy. The platform ingests Reddit discussions, structures unstructured feedback using Spark, enriches it with AI insights using Gemini, and presents executive-ready intelligence through an interactive Streamlit dashboard.

This project demonstrates real-world data engineering practices, lakehouse architecture design, and practical AI integration for business decision support.

Business Problem

Customer feedback related to Amazon Pharmacy is largely unstructured and spread across public forums such as Reddit. Manually tracking this feedback does not scale and often fails to detect early warning signals such as trust erosion, operational issues, or rising negative sentiment.

RxPulse solves this by:

Continuously ingesting customer conversations

Structuring raw text into analytics-ready datasets

Enriching feedback with AI-generated insights

Surfacing actionable intelligence for leadership

Architecture Overview

RxPulse follows a modern Bronze → Silver → Gold lakehouse architecture on Amazon S3.

Ingestion Layer

Reddit API ingestion using Python

Optional web crawling ingestion as a fallback or parallel source

Raw JSON data stored in the Bronze layer on S3

Processing Layer

Apache Spark processes raw data into structured formats

Bronze to Silver transformations clean and normalize the data

Silver to Gold transformations aggregate metrics and prepare AI-ready datasets

Spark can run locally or on EMR without architectural changes

AI Enrichment Layer

Gemini AI analyzes GENERAL_FEEDBACK posts only

Generates sentiment, risk level, hidden theme, and recommended action

AI outputs are written back to the Gold layer in S3

Analytics Layer

AWS Glue Data Catalog manages metadata

Amazon Athena queries Silver and Gold datasets directly from S3

Streamlit dashboard visualizes insights for executives and analysts

Data Layers
Bronze Layer

Raw Reddit and web-scraped JSON

Stored as-is for traceability and reprocessing

Silver Layer

Cleaned and normalized Parquet files

Structured schema suitable for analytics and AI processing

Gold Layer

Aggregated issue metrics

AI-enriched general feedback insights

Executive-ready datasets queried by Athena

AI Enrichment Strategy

AI is applied selectively to maximize value and control cost.

Only general customer feedback is enriched with Gemini AI. For each feedback record, the model produces:

Sentiment (Positive, Neutral, Negative)

Risk level (Low, Medium, High)

Hidden theme

Recommended business action

This ensures AI is used where it provides the highest strategic value rather than blindly across all data.

Streamlit Dashboard Features

The Streamlit dashboard provides an executive-friendly view of customer sentiment and risk.

Key features include:

High-level KPIs such as total mentions and issue categories

Issue volume distribution and issue share visualizations

Executive AI summary highlighting dominant themes and risk levels

High-risk feedback cards with customer evidence

Sentiment distribution for general feedback

Neutral vs negative feedback comparison

Top neutral and top negative feedback detection

High-impact Reddit posts supporting observed trends

CSV export for AI-enriched insights

All dashboard queries run through Amazon Athena on S3 data.

Key Tables

fact_issue_daily
Daily aggregated issue metrics

top_issue_posts
High-impact Reddit posts by issue and date

general_feedback_ai
Gemini-enriched customer feedback with sentiment, risk, and actions

Tech Stack

Python

Apache Spark

Amazon S3

AWS Glue Data Catalog

Amazon Athena

Google Gemini AI

Streamlit

Plotly

Reddit API

Why This Project Matters

RxPulse demonstrates:

Scalable lakehouse design

Real-world ingestion pipelines

Spark-based batch processing

Cost-aware AI integration

Executive-ready analytics delivery

This project reflects the type of systems built by data engineers working on large-scale platforms such as Amazon Pharmacy, Amazon Health, or Amazon Retail Analytics.

Future Enhancements

Near real-time ingestion using streaming services

Automated alerting for high-risk signal spikes

Time-series trend analysis

Additional data sources beyond Reddit

Role-based access control for dashboards

Author

Ashish Kesari
Amazon-Style Data Engineering and AI Analytics