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
