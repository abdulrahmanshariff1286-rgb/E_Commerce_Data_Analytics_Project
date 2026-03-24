# 🛒 E-Commerce Retail Data Pipeline & KPI Engineering In Verulam Blue Miint

This is a comprehensive Data Analytics and SQL Data Engineering portfolio project. It simulates a real-world scenario where an analyst is tasked with taking messy, raw e-commerce transactional data and transforming it into a clean, highly reliable dataset to calculate critical business KPIs.

This project is perfect for:
* 📊 Showcasing advanced SQL skills (CTEs, Window Functions, Regex, Type Casting, Date Parsing)
* 🧹 Demonstrating hands-on ability with Data Cleaning and ETL pipelines
* 💼 Proving strong business acumen by calculating advanced metrics like GMV, Margin Rates, and MoM Growth

## 📌 Project Overview

The goal of this project is to simulate how data professionals ingest raw data, clean anomalies, and construct structured views for business reporting. Using **Advanced SQL**, I built a multi-step data pipeline to:

✅ **Perform Data Profiling** to identify null values, missing dates, and categorical distributions
✅ **Standardize & Parse Data** to handle inconsistent date formats and cast string columns to appropriate data types
✅ **Normalize Anomalies** using Regex to fix common typos in customer segment data (e.g., 'standrad' ➡️ 'standard')
✅ **Filter & Deduplicate** to remove invalid transactions (e.g., negative costs, null amounts) and create a production-ready `clean_table`
✅ **Engineer Business KPIs** to track average order value, gross margins, return rates, and high-value customer behavior

## 📁 Dataset Overview

The dataset contains raw e-commerce transaction logs. It includes common data quality issues (typos, mixed date formats, duplicates) that reflect real-world data engineering challenges.

🧾 **Key Columns:**
* **row_id / date / hour_of_day:** Transaction timing and identification
* **customer_segment:** The tier of the customer (Standard, Premium, Platinum)
* **order_amount_old / cost:** Financial metrics for calculating revenue and margins
* **is_return:** Boolean flag indicating if the order was returned
* **payment_method:** How the transaction was funded

## 🔧 Project Workflow

Here is a step-by-step breakdown of the SQL pipeline built for this project:

### 🔗 1. Data Profiling & Diagnostics
Ran initial diagnostic queries using `SUM(CASE WHEN...)` to identify the exact volume of `NULL` values across critical columns like dates, costs, and payment methods.

### 🔗 2. The "Silver" Parsing Layer
Created intermediate views to cast data types safely. Used `COALESCE` and `try_strptime` to handle multiple mixed date formats (DD.MM.YYYY vs YYYY-MM-DD) present in the raw data.

### 🔗 3. Data Normalization & Cleaning
Applied Regex and `CASE WHEN` logic to clean dirty categorical text, map typos to canonical tiers, and standardize date strings.

```sql
-- Example snippet of the normalization process
CASE
    WHEN customer_segment_raw IS NULL THEN NULL
    WHEN regexp_replace(customer_segment_raw, '[^a-z]', '') IN ('standrad') THEN 'standard'
    WHEN regexp_replace(customer_segment_raw, '[^a-z]', '') IN ('premuim') THEN 'premium'
    WHEN regexp_replace(customer_segment_raw, '[^a-z]', '') IN ('platnum') THEN 'platinum'
    ELSE customer_segment_raw
END AS customer_segment_raw
```

### 🔗 4. Filtering & Deduplication
Enforced strict data quality rules (e.g., `order_amount_old >= 5.0`, `cost > 0`, `hour_of_day BETWEEN 0 AND 23`) and used `SELECT DISTINCT` to generate the final, highly trusted `clean_table`.

### 🔗 5. Business KPI Calculation
Utilized CTEs and Window Functions (`LAG`, `OVER`, `PARTITION BY`) to compute 10 core business metrics:
1. **Average Order Value (AOV)**
2. **Overall Gross Margin %**
3. **Overall Return Rate**
4. **Median Order Amount**
5. **Return Rate by Payment Method**
6. **High-Value Customer GMV Share** (Premium & Platinum segments)
7. **Below-Target Margin Rate** (Flagging orders dropping below predefined margin floors: 40% Standard, 30% Premium, 25% Platinum)
8. **Top-GMV Month**
9. **MoM (Month-over-Month) GMV Growth**
10. **Max MoM Payment-Method Share Shift**

## 👨‍💻 About the Author

Hey, I'm **Abdul Rahman** — an engineering student based in Bangalore and an aspiring data professional. I enjoy breaking down complex datasets into clear, actionable insights and building end-to-end data pipelines.

🚀 **Stay Connected & Check Out My Work**

If you enjoyed this project, let's stay in touch! I regularly share my learning journey and portfolio projects. 

💼 **LinkedIn:** [Your LinkedIn Profile URL]  
🐙 **GitHub:** https://github.com/abdulrahmanshariff1286-rgb  

📂 **Other Projects You Might Like:**
* [Telecom Customer Churn SQL & ML Project](Link-To-Your-Churn-Repo) - An end-to-end pipeline combining SQL database ETL and Python Random Forest modeling to predict customer churn.
