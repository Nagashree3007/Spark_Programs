'''

@Author: Nagashree C R
@Date: 2024-09-07
@Last Modified by: Nagashree C R
@Last Modified: 2024-09-07
@Title : program to create a sample Json file

'''

import json

# Define the sample data
sample_data = [
    {"text": "This is a test sentence for PySpark word count."},
    {"text": "PySpark is a powerful tool for big data processing."},
    {"text": "Spark can handle large-scale data processing efficiently."},
    {"text": "Text processing with Spark is straightforward and effective."},
    {"text": "Using JSON with PySpark allows easy data ingestion."},
    {"text": "Efficient word count can be achieved with Spark transformations."},
    {"text": "This example illustrates how to count words in a dataset."},
    {"text": "Data science workflows often include text analysis."},
    {"text": "Big data platforms like Spark are essential for modern analytics."},
    {"text": "Processing large datasets with Spark can yield valuable insights."},
    {"text": "PySpark simplifies the development of data processing pipelines."},
    {"text": "Analyzing text data can reveal interesting patterns and trends."},
    {"text": "Text mining and natural language processing are common tasks."},
    {"text": "Spark's distributed computing capabilities enhance performance."},
    {"text": "The flexibility of PySpark supports various data processing needs."},
    {"text": "Handling complex data transformations is made easier with Spark."},
    {"text": "Scalable data processing solutions are crucial for handling big data."},
    {"text": "Textual data can be transformed into structured insights using Spark."},
    {"text": "PySpark offers powerful libraries for data manipulation and analysis."},
    {"text": "Data engineers use Spark for large-scale data transformations and queries."}
]

# Write the large data to a JSON file
with open('sample_data.json', 'w') as f:
    json.dump(sample_data, f, indent=4)

print("Large JSON file created successfully.")
