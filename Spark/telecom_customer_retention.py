
# Telecom Customer Retention Analytics
# Spark / PySpark Analysis

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("TelecomCustomerRetentionAnalytics") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

print("Spark Version:", spark.version)
print("Application Name:", spark.sparkContext.appName)
print("Master:", spark.sparkContext.master)
print("Application ID:", spark.sparkContext.applicationId)


# --------------------------------------------------
# 2. Read Telecom Dataset from HDFS
# --------------------------------------------------

input_path = "hdfs://namenode:8020/data/telecom/raw/Telco-Customer-Churn.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)

print("Total Records:", df.count())
print("Total Columns:", len(df.columns))

df.printSchema()


# --------------------------------------------------
# 3. Clean TotalCharges
# --------------------------------------------------

df_clean = df.withColumn(
    "TotalCharges",
    F.when(
        F.trim(F.col("TotalCharges")) == "",
        None
    ).otherwise(
        F.col("TotalCharges").cast("double")
    )
)

print("Missing TotalCharges:",
      df_clean.filter(F.col("TotalCharges").isNull()).count())


# --------------------------------------------------
# 4. Create Churn Flag
# --------------------------------------------------

df_clean = df_clean.withColumn(
    "churn_flag",
    F.when(F.col("Churn") == "Yes", 1).otherwise(0)
)


# --------------------------------------------------
# 5. Create Tenure Groups
# --------------------------------------------------

df_clean = df_clean.withColumn(
    "tenure_group",
    F.when(F.col("tenure").between(0, 12), "0-12")
     .when(F.col("tenure").between(13, 24), "13-24")
     .when(F.col("tenure").between(25, 48), "25-48")
     .otherwise("49-72")
)


# --------------------------------------------------
# 6. Create Charge Bands
# --------------------------------------------------

df_clean = df_clean.withColumn(
    "charge_band",
    F.when(F.col("MonthlyCharges") < 50, "Low")
     .when(F.col("MonthlyCharges") < 80, "Medium")
     .otherwise("High")
)


# --------------------------------------------------
# 7. Overall Churn Analysis
# --------------------------------------------------

overall_result = df_clean.agg(
    F.count("*").alias("total_customers"),
    F.sum("churn_flag").alias("churned_customers"),
    F.round(
        F.avg("churn_flag") * 100,
        2
    ).alias("churn_rate")
)

print("\n========== OVERALL CHURN ==========")
overall_result.show()


# --------------------------------------------------
# 8. Churn by Contract
# --------------------------------------------------

contract_result = df_clean.groupBy("Contract").agg(
    F.count("*").alias("total_customers"),
    F.sum("churn_flag").alias("churned_customers"),
    F.round(
        F.avg("churn_flag") * 100,
        2
    ).alias("churn_rate")
).orderBy(
    F.desc("churn_rate")
)

print("\n========== CHURN BY CONTRACT ==========")
contract_result.show()


# --------------------------------------------------
# 9. Churn by Tenure Group
# --------------------------------------------------

tenure_result = df_clean.groupBy("tenure_group").agg(
    F.count("*").alias("total_customers"),
    F.sum("churn_flag").alias("churned_customers"),
    F.round(
        F.avg("churn_flag") * 100,
        2
    ).alias("churn_rate")
).orderBy(
    F.desc("churn_rate")
)

print("\n========== CHURN BY TENURE ==========")
tenure_result.show()


# --------------------------------------------------
# 10. Q4 - Service and Payment Analysis
# --------------------------------------------------

service_payment_result = df_clean.groupBy(
    "InternetService",
    "PaymentMethod"
).agg(
    F.count("*").alias("total_customers"),
    F.sum("churn_flag").alias("churned_customers"),
    F.round(
        F.avg("churn_flag") * 100,
        2
    ).alias("churn_rate")
).orderBy(
    F.desc("churn_rate")
)

print("\n========== Q4: SERVICE + PAYMENT ==========")
service_payment_result.show(20, truncate=False)


# --------------------------------------------------
# 11. Q5 - Customer Risk Ranking
# --------------------------------------------------

risk_window = Window.orderBy(
    F.desc("churn_rate")
)

risk_ranking = service_payment_result.withColumn(
    "risk_rank",
    F.row_number().over(risk_window)
)

print("\n========== Q5: RISK RANKING ==========")
risk_ranking.show(20, truncate=False)


# --------------------------------------------------
# 12. Display Highest-Risk Segment
# --------------------------------------------------

print("\n========== HIGHEST RISK SEGMENT ==========")

risk_ranking.filter(
    F.col("risk_rank") == 1
).show(truncate=False)


# --------------------------------------------------
# 13. Save Results to HDFS
# --------------------------------------------------

contract_output = "/data/telecom/results/spark_churn_by_contract"
tenure_output = "/data/telecom/results/spark_churn_by_tenure"
service_payment_output = "/data/telecom/results/spark_service_payment"
risk_output = "/data/telecom/results/spark_risk_ranking"

contract_result.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(contract_output)

tenure_result.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(tenure_output)

service_payment_result.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(service_payment_output)

risk_ranking.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(risk_output)


# --------------------------------------------------
# 14. Completion Message
# --------------------------------------------------

print("\n========================================")
print("Spark analysis completed successfully.")
print("Results stored in HDFS.")
print("========================================")


# Stop Spark

spark.stop()
