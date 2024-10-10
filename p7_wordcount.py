'''

@Author: Nagashree C R
@Date: 2024-09-07
@Last Modified by: Nagashree C R
@Last Modified: 2024-09-07
@Title : Program to find count of words in Json file 

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("WordCountExample") \
    .getOrCreate()

# Load JSON data into DataFrame with error handling
df = spark.read.option("mode", "DROPMALFORMED").json(r"C:\Users\ASHAY\OneDrive\Desktop\bridgelabz\spark\Spark_programs\sample_data.json")

# Check if _corrupt_record exists and handle it
if "_corrupt_record" in df.columns:
    df_clean = df.filter(df["_corrupt_record"].isNull())
else:
    df_clean = df

# Show the cleaned DataFrame
df_clean.show(truncate=False)

# Tokenize the text column into words
words_df = df_clean.select(explode(split(col("text"), " ")).alias("word"))

# Group by the word and count occurrences
word_counts = words_df.groupBy("word").count()

# Show the results
word_counts.show()

# Stop the SparkSession
spark.stop()
