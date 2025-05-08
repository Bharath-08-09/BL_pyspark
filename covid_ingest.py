from pyspark.sql import SparkSession

# Initialize Spark Session with PostgreSQL JDBC driver
spark = SparkSession.builder \
    .appName("CovidExcessDeathsIngestion") \
    .config("spark.jars", "D:\pyspark\postgresql-42.7.5.jar") \
    .getOrCreate()

# Load CSV into Spark DataFrame
df = spark.read.option("header", True).csv("D:/pyspark/all_weekly_excess_deaths.csv")

# Clean column names
for col in df.columns:
    df = df.withColumnRenamed(col, col.lower().replace(" ", "_"))

# Write to PostgreSQL database
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/Covid_19db") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "weekly_excess_deaths") \
    .option("user", "postgres") \
    .option("password", "Bharath08") \
    .mode("overwrite") \
    .save()

print("✅ Data successfully loaded into PostgreSQL.")