# RxPulse – Amazon Pharmacy Voice of Customer Intelligence Platform



RxPulse is an end-to-end data engineering and AI-driven analytics platform designed to capture, process, and surface customer voice signals for Amazon Pharmacy. The platform ingests Reddit discussions, structures unstructured feedback using Apache Spark, enriches it with AI insights using Gemini, and presents executive-ready intelligence through an interactive Streamlit dashboard.

This project demonstrates real-world data engineering practices, modern lakehouse architecture design, and practical AI integration for business decision support at Amazon scale.

---

## Business Problem

Customer feedback related to Amazon Pharmacy is largely unstructured and spread across public forums such as Reddit. Manually tracking this feedback does not scale and often fails to detect early warning signals such as trust erosion, operational issues, fulfillment problems, or rising negative sentiment.

RxPulse solves this problem by building an automated intelligence pipeline that continuously listens to customer conversations and converts raw text into structured, decision-ready insights.

---

## What RxPulse Does

RxPulse enables Amazon Pharmacy teams to:

- Continuously ingest customer conversations from public forums  
- Structure raw text into analytics-ready datasets  
- Enrich feedback with AI-generated sentiment, risk, and themes  
- Surface actionable intelligence for leadership and operations teams  

---

## Architecture Overview

RxPulse follows a modern **Bronze → Silver → Gold lakehouse architecture** built on Amazon S3.

![Architecture Diagram](rxpulse/images/architecture diagram.png)

---

## Ingestion Layer

- Reddit API ingestion using Python  
- Optional web crawling ingestion as a fallback or parallel source  
- Raw JSON data stored in the Bronze layer on Amazon S3  

---

## Processing Layer

- Apache Spark processes raw data into structured formats  
- Bronze to Silver transformations clean, normalize, and standardize data  
- Silver to Gold transformations aggregate metrics and prepare AI-ready datasets  
- Spark can run locally or on Amazon EMR without architectural changes  

---

## AI Enrichment Layer

- Gemini AI analyzes **GENERAL_FEEDBACK** posts only  
- For each feedback record, the model generates:
  - Sentiment (Positive, Neutral, Negative)  
  - Risk level (Low, Medium, High)  
  - Hidden customer theme  
  - Recommended business action  
- AI outputs are written back to the Gold layer in Amazon S3  

This selective AI strategy ensures high business value while controlling cost.

---

## Analytics Layer

- AWS Glue Data Catalog manages metadata  
- Amazon Athena queries Silver and Gold datasets directly from S3  
- Streamlit dashboard presents executive-ready intelligence  

---

## Data Layers

### Bronze Layer
- Raw Reddit and web-scraped JSON  
- Stored as-is for traceability and reprocessing  

### Silver Layer
- Cleaned and normalized Parquet datasets  
- Structured schema suitable for analytics and AI processing  

### Gold Layer
- Aggregated issue metrics  
- AI-enriched general feedback insights  
- Executive-ready datasets queried by Athena  

---

## Streamlit Dashboard

The Streamlit dashboard provides an executive-friendly view of customer sentiment, operational risks, and emerging issues.

![Dashboard Overview](rxpulse/images/1.png)

### Key Dashboard Features

- High-level KPIs such as total mentions and issue categories  
- Issue volume distribution and issue share visualizations  
- Executive AI summary highlighting dominant themes and risks  
- High-risk feedback cards with customer evidence  

![Sentiment Analysis](rxpulse/images/2.png)

- Sentiment distribution for general feedback  
- Neutral vs negative feedback comparison  
- Detection of top neutral and top negative feedback  

![High Risk Feedback](rxpulse/images/3.png)

- Identification of high-impact Reddit posts  
- Customer evidence supporting observed trends  

![Export and Insights](rxpulse/images/4.png)

- CSV export for AI-enriched insights  
- All queries executed via Amazon Athena on S3 data  

---

## Key Tables

- **fact_issue_daily**  
  Daily aggregated issue metrics  

- **top_issue_posts**  
  High-impact Reddit posts by issue and date  

- **general_feedback_ai**  
  Gemini-enriched customer feedback with sentiment, risk, and recommended actions  

---

## Tech Stack

- Python  
- Apache Spark  
- Amazon S3  
- AWS Glue Data Catalog  
- Amazon Athena  
- Google Gemini AI  
- Streamlit  
- Plotly  
- Reddit API  

---

## Why This Project Matters

RxPulse demonstrates:

- Scalable lakehouse architecture design  
- Real-world ingestion pipelines  
- Spark-based batch processing  
- Cost-aware AI integration  
- Executive-ready analytics delivery  

This project reflects the type of systems built by data engineers working on large-scale platforms such as Amazon Pharmacy, Amazon Health, and Amazon Retail Analytics.

---

## Future Enhancements

- Near real-time ingestion using streaming services  
- Automated alerting for high-risk signal spikes  
- Time-series trend analysis  
- Additional data sources beyond Reddit  
- Role-based access control for dashboards  

---

## Author

**Ashish Kesari**  
Amazon-Style Data Engineering and AI Analytics
