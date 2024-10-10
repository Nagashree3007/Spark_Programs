'''

@Author: Nagashree C R
@Date: 2024-09-07
@Last Modified by: Nagashree C R
@Last Modified: 2024-09-07
@Title : Program to find out the countries and continents with the highest death counts

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sql_sum, when, round

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DeathRateCalculation") \
    .getOrCreate()

# Load your data into a DataFrame (adjust the file path accordingly)
df = spark.read.csv(r"C:\Users\ASHAY\Downloads\covid-kaggle-dataset\worldometer_data.csv", header=True, inferSchema=True)

# Print schema to verify column names
df.printSchema()

# Calculate global death rate and find the country with the highest death rate
df_death_rate = df.groupBy("Country") \
    .agg(
        sql_sum(col("TotalCases").cast("float")).alias("total_cases"),
        sql_sum(col("Deaths").cast("float")).alias("total_deaths")
    ) \
    .withColumn("global_death_rate",
        when(col("total_cases") > 0, round((col("total_deaths") * 100.0) / col("total_cases"), 2))
        .otherwise(0)
    ) \
    .select("Country", "global_death_rate") \
    .orderBy(col("global_death_rate").desc()) \
    .limit(1)

# Show results
print("Country with the highest death rate:")
df_death_rate.show()

# Stop SparkSession
spark.stop()
