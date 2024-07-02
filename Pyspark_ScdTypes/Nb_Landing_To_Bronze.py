# Databricks notebook source

##########################Landing_to_Bronze#############################################
from pyspark.sql import *
from pyspark.sql.types import *
from datetime import datetime
import os

# List of paths
Paths = dbutils.fs.ls('/Project/Landing')
path_list = [item.path for item in Paths]

# Define schema for DataFrame
schema = StructType([StructField("path", StringType(), True)])

# Create DataFrame
df = spark.createDataFrame([(path,) for path in path_list], schema)

# Show DataFrame
# df.show(truncate=False)



def Landing_To_Bronze(df):
    for row in df.collect():
        file_path = row.path
        file_name = os.path.basename(file_path).split(".")[0]
        df = spark.read.csv(file_path, header=True)
        today_date = datetime.now()
        year = today_date.year
        month = today_date.month
        day = today_date.day

        # Define the output path
        output_path = f"dbfs:/Project/Bronze/{file_name}/{year}/{month}/{day}/"
        try:
            df.write.csv(output_path, mode="overwrite", header=True)
            # df.show()
        except Exception as e:
            print(f"An error occurred while writing the DataFrame to {output_path}: {e}")



# Apply the function
Landing_To_Bronze(df)

