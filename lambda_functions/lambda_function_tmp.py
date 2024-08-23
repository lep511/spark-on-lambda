from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from datetime import datetime, date
import json
import sys
import os
      
search_path = sys.path
print(search_path)

spark = SparkSession.builder \
    .appName("Spark-on-AWS-Lambda") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.memory", "5g") \
    .config("spark.executor.memory", "5g") \
    .enableHiveSupport().getOrCreate()

def lambda_handler(event, context):

    dataset = spark.read.option('header', 'true').option("delimiter", ";").csv("accommodations.csv")