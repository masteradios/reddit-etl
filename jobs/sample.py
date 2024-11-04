from pyspark.sql import SparkSession
import s3fs
import tempfile
import pandas as pd
import os
import openpyxl
# Read the input text file



# # Download the Excel file from S3 to a temporary local file
# with s3.open(s3_object_path, 'rb') as s3_file:
#     with tempfile.NamedTemporaryFile(delete=False) as temp_file:
#         temp_file.write(s3_file.read())
#         temp_file_path = temp_file.name

# Read data from the temporary local Excel file into a Spark DataFrame
df = pd.read_excel('jobs/maharastra-AC-2019.xlsx') # Assuming the second path is the CSV file

#Show the DataFrame
print("version is ",openpyxl.__version__)


# Get the current working directory
current_directory = os.getcwd()

# Get the name of the current directory
directory_name = os.path.basename(current_directory)

print(directory_name)
    # Stop the Spark session
df.to_csv('mahaelections.csv')
spark.stop()



# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

# text = "Hello Spark Hello Python Hello Airflow Hello Docker and Hello Yusuf"

# words = spark.sparkContext.parallelize(text.split(" "))

# wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# for wc in wordCounts.collect():
#     print(wc[0], wc[1])

# spark.stop()