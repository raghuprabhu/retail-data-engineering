# retail-data-engineering
# End-to-End Retail Customer Analytics Pipeline on GCP using PySpark

This project demonstrates an end-to-end data engineering pipeline built using PySpark on Google Cloud Platform (GCP). The data simulates retail transactions, customer info, reviews, and interactions.

---

## **Project Objectives**

- Ingest CSV files from GCS
- Define custom schema using StructType
- Handle corrupt/bad records gracefully
- Clean, transform, and enrich data using PySpark
- Apply various types of joins (inner, left, broadcast)
- Use window functions to analyze customer behavior
- Apply performance tuning (repartition, coalesce, cache)
- Write final output to GCS / BigQuery

---

## **Technologies Used**

- **Google Cloud Storage (GCS)** – for raw and processed data
- **Dataproc (PySpark)** – for data processing and transformation
- **BigQuery** *(optional)* – for storing output
- **Airflow** *(optional)* – for orchestration
- **Python & PySpark**

---

## **Dataset Overview**

The dataset includes the following CSV files:

- `customers.csv`
- `transactions.csv`
- `campaigns.csv`
- `customer_reviews.csv`
- `interactions.csv`
- `support_tickets.csv`

Source: [Kaggle Retail Dataset](#) *(https://www.kaggle.com/datasets/raghavendragandhi/retail-customer-and-transaction-dataset)*

---

## **Key PySpark Concepts Used**

- Schema definition using `StructType`
- Handling corrupt records using `mode` & `badRecordsPath`
- Data cleaning: `dropna`, `fillna`, trimming
- Joins: inner, left, broadcast
- Window functions: `row_number`, `rank`, `lag`
- Transformations: `withColumn`, `selectExpr`, `groupBy`
- Performance: `repartition`, `coalesce`, `cache`, `persist`

---

## **Execution**

Run on Dataproc:

```bash
spark-submit scripts/retail_pipeline.py
