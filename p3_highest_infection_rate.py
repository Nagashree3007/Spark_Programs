from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sql_sum, when, round

# Initialize SparkSession
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InfectionRateCalculation") \
    .config("spark.ui.port", "4050")  \
    .getOrCreate()


# Load your data into a DataFrame (adjust the file path accordingly)
df = spark.read.csv(r"C:\Users\ASHAY\Downloads\covid-kaggle-dataset\worldometer_data.csv", header=True, inferSchema=True)

# Print schema to verify column names
df.printSchema()

# Calculate infection rate and find the country with the highes
