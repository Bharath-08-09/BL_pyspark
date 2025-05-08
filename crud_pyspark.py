import os
from pyspark.sql import SparkSession

# Update this to your actual Python path
os.environ["PYSPARK_PYTHON"] = r"C:\Users\Bharath S\anaconda3\envs\myenv\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Bharath S\anaconda3\envs\myenv\python.exe"

# Create Spark session with limited memory
spark = SparkSession.builder \
    .appName("FixMemoryAndPython") \
    .master("local[1]") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

df = spark.read.csv("D:/pyspark/all_weekly_excess_deaths.csv", header=True, inferSchema=True)
df.show(5)


from pyspark.sql import Row

# Define a new row matching the schema
new_row = Row(
    country="Testland", region="TestRegion", region_code=999, start_date="2025-01-01", end_date="2025-01-07",
    days=7, year=2025, week=1, population=1000000, total_deaths=150.0, covid_deaths=5,
    expected_deaths=140.0, excess_deaths=10.0, non_covid_deaths=145.0,
    covid_deaths_per_100k=0.5, excess_deaths_per_100k=1.0, excess_deaths_pct_change=0.07
)

# Append new row to existing DataFrame
df = df.union(spark.createDataFrame([new_row]))

df.orderBy("year", ascending=False).show(1)

df.filter(df["covid_deaths"] > 50).select("country", "region", "covid_deaths").show()

from pyspark.sql.functions import when, col

df = df.withColumn(
    "expected_deaths",
    when(col("year") == 2020, col("expected_deaths") * 1.1).otherwise(col("expected_deaths"))
)

df.filter(col("year") == 2020).select("year", "expected_deaths").show(5)

df = df.filter(col("region") != "TestRegion")
df.show(5)