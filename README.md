# Telecom Customer Retention Analytics

## Project Overview

This project analyzes telecom customer data to identify customer churn patterns and factors associated with customer retention.

The project uses Big Data technologies to process and analyze the IBM Telco Customer Churn dataset.

## Technologies Used

- HDFS – Distributed storage
- Hive – SQL-based data analysis
- YARN – Cluster resource management
- Apache Spark / PySpark – Data processing and analytics
- HBase – NoSQL storage for churn segment results
- Jupyter Notebook – PySpark development
- Docker – Big Data environment

## Dataset

**Dataset:** IBM Telco Customer Churn Dataset

- Records: 7,043
- Features: 21
- Target variable: `Churn`

The dataset contains customer information such as tenure, contract type, internet service, payment method, monthly charges, total charges, and churn status.

## Analytics Questions

The project focuses on the following questions:

1. What is the overall customer churn rate?
2. Which contract type has the highest churn rate?
3. How does customer tenure affect churn?
4. Which combination of internet service and payment method has the highest churn?
5. Which customer segment is at the highest churn risk?

## Project Workflow

```text
Telco Customer Churn Dataset
            ↓
           HDFS
            ↓
           Hive
            ↓
           YARN
            ↓
      Spark / PySpark
            ↓
       HDFS Results
            ↓
           HBase
            ↓
     Churn Insights
```


## Key Results

- Overall customer churn rate: **26.54%**
- Month-to-month contract churn rate: **42.71%**
- One-year contract churn rate: **11.27%**
- Two-year contract churn rate: **2.83%**
- Highest tenure-based churn: **0–12 months (47.44%)**
- Highest-risk service/payment segment: **Fiber optic + Electronic check (53.23%)**
- Highest-risk segment rank: **Rank 1**

## Spark Processing

PySpark was used to process the telecom dataset read from HDFS.

Main processing steps:

- Read the raw CSV dataset from HDFS.
- Converted `TotalCharges` from string to numeric format.
- Handled 11 missing `TotalCharges` values.
- Created a binary `churn_flag` column.
- Calculated churn rates by contract, tenure, internet service, and payment method.
- Applied risk ranking to identify the highest-risk customer segment.
- Stored Spark analytical results back in HDFS.

## HBase Storage

HBase was used to store the final churn segment analytics.

- Table: `churn_segments`
- Column family: `metrics`
- Row keys represent overall churn, contract segments, and tenure segments.
- Stored metrics include total customers, churned customers, and churn rate.
- HBase scan was used to verify the stored analytical results.

## HDFS Storage

HDFS was used as the distributed storage layer for the telecom dataset and analytical results.

- Raw dataset stored at:
  `/data/telecom/raw/Telco-Customer-Churn.csv`
- Processed data and analytical results were stored under:
  `/data/telecom/results/`
- HDFS was used to verify file availability, storage, and generated result files.

## Hive Analytics

Hive was used to perform SQL-based churn analysis on the data stored in HDFS.

The following analyses were performed:

- Overall customer churn rate
- Churn rate by contract type
- Churn rate by customer tenure

Hive results were stored in HDFS for further use in the project.

## Team Contributions

- **Member 1:** HDFS setup, dataset ingestion, Hive database and table creation, Hive analytics
- **Member 2:** Spark/PySpark data processing, cleaning, churn analysis, risk ranking
- **Member 3:** HBase integration and churn segment storage
- **Member 4:** Project documentation, visualization, and presentation
- **Member 5:** Project coordination, testing, and final integration
