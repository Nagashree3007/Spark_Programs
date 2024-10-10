'''

@Author: Nagashree C R
@Date: 2024-09-07
@Last Modified by: Nagashree C R
@Last Modified: 2024-09-07
@Title : To find out the infected population percentage locally and globally

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sql_sum, when, round

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("InfectionCalculation") \
    .getOrCreate()

# Load your data into a DataFrame (adjust the file path accordingly)
df = spark.read.csv(r"C:\Users\ASHAY\Downloads\covid-kaggle-dataset\worldometer_data.csv", header=True, inferSchema=True)

# Print schema to verify column names
df.printSchema()

# Global infection percentage calculation
df_global = df.groupBy("Country") \
    .agg(
        sql_sum(col("TotalCases").cast("float")).alias("confirmed_cases_global"),
        sql_sum(col("Population").cast("float")).alias("Total_population")
    ) \
    .withColumn("global_infected_percentage",
        when(col("confirmed_cases_global") > 0, round((col("confirmed_cases_global") * 100.0) / col("Total_population"), 2))
        .otherwise(0)
    ) \
    .select("Country", "global_infected_percentage") \
    .orderBy("Country")

df_local=df.groupby('Country')\
    .agg(
        sql_sum(col('TotalCases').cast('float')).alias('confirmed_cases_local'),
        sql_sum(col('Population').cast('float')).alias('Total_population')
    )\
    .withColumn('local_infected_percentage',
        when(col('confirmed_cases_local')>0, round((col("confirmed_cases_local") * 100.0) / col("Total_population"), 2))
        .otherwise(0))\
    .select("Country", "local_infected_percentage") \
    .orderBy("Country")
# Show results
print("Global_Infected_Percentage ")
df_global.show()
print("localy_Infected_Percentage ")
df_local.show()

# Stop SparkSession
spark.stop()
