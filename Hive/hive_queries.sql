
-- Telecom Customer Retention Analytics
-- Hive Database and Customer Churn Analysis


-- 1. Create database

CREATE DATABASE IF NOT EXISTS telecom_analytics;

USE telecom_analytics;


-- 2. Create external table

CREATE EXTERNAL TABLE IF NOT EXISTS customer_churn (
    customerID STRING,
    gender STRING,
    SeniorCitizen INT,
    Partner STRING,
    Dependents STRING,
    tenure INT,
    PhoneService STRING,
    MultipleLines STRING,
    InternetService STRING,
    OnlineSecurity STRING,
    OnlineBackup STRING,
    DeviceProtection STRING,
    TechSupport STRING,
    StreamingTV STRING,
    StreamingMovies STRING,
    Contract STRING,
    PaperlessBilling STRING,
    PaymentMethod STRING,
    MonthlyCharges DOUBLE,
    TotalCharges STRING,
    Churn STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION '/data/telecom/raw'
TBLPROPERTIES ("skip.header.line.count"="1");


-- 3. Verify total number of customers

SELECT COUNT(*) AS total_customers
FROM telecom_analytics.customer_churn;


-- Q1. Overall churn analysis

SELECT COUNT(*) AS total_customers,
       SUM(CASE WHEN Churn='Yes' THEN 1 ELSE 0 END) AS churned_customers,
       ROUND(
           SUM(CASE WHEN Churn='Yes' THEN 1 ELSE 0 END)*100.0/COUNT(*),
           2
       ) AS churn_percentage
FROM telecom_analytics.customer_churn;


-- Q2. Churn rate by contract type

SELECT Contract,
       COUNT(*) AS total_customers,
       SUM(CASE WHEN Churn='Yes' THEN 1 ELSE 0 END) AS churned_customers,
       ROUND(
           SUM(CASE WHEN Churn='Yes' THEN 1 ELSE 0 END)*100.0/COUNT(*),
           2
       ) AS churn_rate
FROM telecom_analytics.customer_churn
GROUP BY Contract
ORDER BY churn_rate DESC;


-- Q3. Churn rate by tenure group

SELECT
    CASE
        WHEN tenure BETWEEN 0 AND 12 THEN '0-12'
        WHEN tenure BETWEEN 13 AND 24 THEN '13-24'
        WHEN tenure BETWEEN 25 AND 48 THEN '25-48'
        ELSE '49-72'
    END AS tenure_group,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN Churn='Yes' THEN 1 ELSE 0 END) AS churned_customers,
    ROUND(
        SUM(CASE WHEN Churn='Yes' THEN 1 ELSE 0 END)*100.0/COUNT(*),
        2
    ) AS churn_rate
FROM telecom_analytics.customer_churn
GROUP BY
    CASE
        WHEN tenure BETWEEN 0 AND 12 THEN '0-12'
        WHEN tenure BETWEEN 13 AND 24 THEN '13-24'
        WHEN tenure BETWEEN 25 AND 48 THEN '25-48'
        ELSE '49-72'
    END
ORDER BY churn_rate DESC;
