import os
import time
import streamlit as st
import pandas as pd
import boto3
import plotly.express as px
from dotenv import load_dotenv


# 1. CONFIG

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
ATHENA_DB = os.getenv("ATHENA_DB", "rxpulse")
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT", "s3://amzn-rx/athena-results/")

# 2.ATHENA HELPER

def query_athena(sql: str) -> pd.DataFrame:
    athena = boto3.client("athena", region_name=AWS_REGION)

    res = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )

    qid = res["QueryExecutionId"]

    while True:
        state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(0.4)

    if state != "SUCCEEDED":
        raise RuntimeError("Athena query failed")

    result = athena.get_query_results(QueryExecutionId=qid)
    cols = [c["Label"] for c in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = [[d.get("VarCharValue") for d in r["Data"]] for r in result["ResultSet"]["Rows"][1:]]

    return pd.DataFrame(rows, columns=cols)


# 3.PAGE SETUP 

st.set_page_config(
    page_title="RxPulse – Amazon Pharmacy VoC",
    page_icon="💊",
    layout="wide"
)


# 4. STYLES

st.markdown("""
<style>
.big-title { font-size:44px; font-weight:900; }
.subtitle { color:#9ca3af; font-size:16px; margin-top:-6px; }
.kpi-box {
    background:#111827; padding:20px; border-radius:14px;
    text-align:center; border:1px solid rgba(255,255,255,0.06);
}
.kpi-number { font-size:38px; font-weight:800; color:#f59e0b; }
.kpi-label { color:#9ca3af; font-size:14px; }
.card {
    background:#0b1220; padding:18px; border-radius:14px;
    border:1px solid rgba(255,255,255,0.06);
}
.small-muted { color:#9ca3af; font-size:13px; }
</style>
""", unsafe_allow_html=True)


# 5. HEADER

st.markdown('<div class="big-title">💊 RxPulse</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="subtitle">Amazon Pharmacy – Voice of Customer Intelligence Platform (Spark + S3 + Athena + Gemini)</div>',
    unsafe_allow_html=True
)
st.markdown("---")

# 6.DATE SELECTOR

dates = query_athena("""
SELECT DISTINCT dt
FROM rxpulse.fact_issue_daily
ORDER BY dt DESC
""")

if dates.empty:
    st.error("No data found in Athena.")
    st.stop()

selected_dt = st.selectbox("📅 Select Snapshot Date", dates["dt"])

# 7. KPI SECTION

kpis = query_athena(f"""
SELECT
  SUM(mentions) AS total_mentions,
  COUNT(DISTINCT issue_type) AS issue_types
FROM rxpulse.fact_issue_daily
WHERE dt = '{selected_dt}'
""")

total_mentions = int(float(kpis.iloc[0]["total_mentions"]))
issue_types = int(float(kpis.iloc[0]["issue_types"]))

c1, c2, c3 = st.columns(3)

with c1:
    st.markdown(f"""
    <div class="kpi-box">
        <div class="kpi-number">{total_mentions}</div>
        <div class="kpi-label">Customer Mentions</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="kpi-box">
        <div class="kpi-number">{issue_types}</div>
        <div class="kpi-label">Issue Categories</div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class="kpi-box">
        <div class="kpi-number">Reddit</div>
        <div class="kpi-label">Primary Signal Source</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# 8. ISSUE DISTRIBUTION

issue_df = query_athena(f"""
SELECT issue_type, mentions
FROM rxpulse.fact_issue_daily
WHERE dt = '{selected_dt}'
ORDER BY CAST(mentions AS BIGINT) DESC
""")
issue_df["mentions"] = issue_df["mentions"].astype(int)

l, r = st.columns([2,1])

with l:
    bar = px.bar(issue_df, x="issue_type", y="mentions", text="mentions",
                 template="plotly_dark", title="📊 Issue Volume Distribution")
    bar.update_layout(showlegend=False)
    st.plotly_chart(bar, use_container_width=True)

with r:
    donut = px.pie(issue_df, names="issue_type", values="mentions",
                   hole=0.55, template="plotly_dark", title="🧠 Issue Share")
    st.plotly_chart(donut, use_container_width=True)

st.markdown("---")


# 9. EXECUTIVE AI INSIGHTS

st.subheader("🧠 Executive AI Insights (General Feedback)")

ai_df = query_athena(f"""
SELECT sentiment, risk_level, hidden_theme, action, title
FROM rxpulse.general_feedback_ai
WHERE dt = '{selected_dt}'
""")

ai_df = ai_df.astype(str)
high_risk = ai_df[ai_df["risk_level"].str.lower() == "high"]

sentiment_mode = ai_df["sentiment"].value_counts().idxmax()
top_theme = ai_df["hidden_theme"].value_counts().idxmax()

# Summary Cards
s1, s2, s3 = st.columns(3)
s1.metric("Overall Sentiment", sentiment_mode)
s2.metric("Dominant Theme", top_theme)
s3.metric("High-Risk Signals", len(high_risk))

st.markdown("### 🚨 High-Risk Signals")
for _, r in high_risk.iterrows():
    st.warning(f"""
**Risk Theme:** {r['hidden_theme']}  
**Recommended Action:** {r['action']}  
**Customer Evidence:**  
{r['title']}
""")

st.markdown("---")


# 10. NEUTRAL vs NEGATIVE COMPARISON

st.subheader("⚖️ Neutral vs Negative Feedback")

sent_df = query_athena(f"""
SELECT sentiment, COUNT(*) AS cnt
FROM rxpulse.general_feedback_ai
WHERE dt = '{selected_dt}'
GROUP BY sentiment
""")
sent_df["cnt"] = sent_df["cnt"].astype(int)

sent_bar = px.bar(
    sent_df,
    x="sentiment",
    y="cnt",
    color="sentiment",
    template="plotly_dark",
    title="Sentiment Comparison"
)
st.plotly_chart(sent_bar, use_container_width=True)


# 11. TOP NEUTRAL & NEGATIVE POSTS

c1, c2 = st.columns(2)

with c1:
    st.subheader("😐 Top Neutral Feedback")
    neutral_df = query_athena(f"""
    SELECT title, hidden_theme
    FROM rxpulse.general_feedback_ai
    WHERE dt = '{selected_dt}' AND sentiment = 'Neutral'
    LIMIT 10
    """)
    st.dataframe(neutral_df, use_container_width=True, hide_index=True)

with c2:
    st.subheader("😠 Top Negative Feedback")
    negative_df = query_athena(f"""
    SELECT title, hidden_theme
    FROM rxpulse.general_feedback_ai
    WHERE dt = '{selected_dt}' AND sentiment = 'Negative'
    LIMIT 10
    """)
    st.dataframe(negative_df, use_container_width=True, hide_index=True)

st.markdown("---")


# 12. TOP POSTS

st.subheader("🧵 High-Impact Customer Posts")

posts = query_athena(f"""
SELECT issue_type, title, created_date, source
FROM rxpulse.top_issue_posts
WHERE dt = '{selected_dt}'
ORDER BY created_date DESC
LIMIT 25
""")

st.dataframe(posts, use_container_width=True, hide_index=True)

st.markdown("---")
st.caption("Built by Ashish Kesari | Amazon-Style Data Engineering & AI Platform")
