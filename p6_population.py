'''

@Author: Nagashree C R
@Date: 2024-09-07
@Last Modified by: Nagashree C R
@Last Modified: 2024-09-07
@Title : Program to get Average of cases divided by the number of population of each country (TOP 10) 

'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sql_sum, when, round

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Top10CountriesByInfectionRate") \
    .getOrCreate()

# Load your data into a DataFrame (adjust the file path accordingly)
df = spark.read.csv(r"C:\Users\ASHAY\Downloads\covid-kaggle-dataset\worldometer_data.csv", header=True, inferSchema=True)

# Print schema to verify column names
df.printSchema()

# Calculate infection rate and find the top 10 countries
df_infection_rate = df.groupBy("Country") \
    .agg(
        sql_sum(col("TotalCases").cast("float")).alias("total_cases"),
        sql_sum(col("Population").cast("float")).alias("total_population")
    ) \
    .withColumn("infection_rate",
        when(col("total_population") > 0, round((col("total_cases") * 100.0) / col("total_population"), 2))
        .otherwise(0)
    ) \
    .select("Country", "infection_rate") \
    .orderBy(col("infection_rate").desc()) \
    .limit(10)

# Show results
print("Top 10 countries by infection rate:")
df_infection_rate.show()

# Stop SparkSession
spark.stop()
