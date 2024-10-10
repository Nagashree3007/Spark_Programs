'''

@Author: Nagashree C R
@Date: 2024-09-07
@Last Modified by: Nagashree C R
@Last Modified: 2024-09-07
@Title : To find out the death percentage locally and globally

'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sql_sum, when, round

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DeathPercentageCalculation") \
    .getOrCreate()

# Load your data into a DataFrame (adjust the file path accordingly)
df = spark.read.csv(r"C:\Users\ASHAY\Downloads\covid-kaggle-dataset\covid_19_clean_complete.csv", header=True, inferSchema=True)

# Print schema to verify column names
df.printSchema()

# Global death percentage calculation
df_global = df.groupBy("Country/Region") \
    .agg(
        sql_sum(col("Confirmed").cast("float")).alias("Total_Confirmed"),
        sql_sum(col("Deaths").cast("float")).alias("Total_Deaths")
    ) \
    .withColumn("global_death_percentage",
        when(col("Total_Confirmed") > 0, round((col("Total_Deaths") * 100.0) / col("Total_Confirmed"), 2))
        .otherwise(0)
    ) \
    .select("Country/Region", "global_death_percentage") \
    .orderBy("Country/Region")

# Local death percentage calculation for a specific country (e.g., India)
df_local = df.filter(col("Country/Region") == 'India') \
    .groupBy("Country/Region") \
    .agg(
        sql_sum(col("Confirmed").cast("float")).alias("Total_Confirmed"),
        sql_sum(col("Deaths").cast("float")).alias("Total_Deaths")
    ) \
    .withColumn("local_death_percentage",
        when(col("Total_Confirmed") > 0, round((col("Total_Deaths") * 100.0) / col("Total_Confirmed"), 2))
        .otherwise(0)
    ) \
    .select("Country/Region", "local_death_percentage") \
    .orderBy("Country/Region")

# Show results
print("Global Death Percentages:")
df_global.show()

print("Local Death Percentages (India):")
df_local.show()

# Stop the SparkSession
spark.stop()
