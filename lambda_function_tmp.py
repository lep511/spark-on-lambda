import boto3
import botocore
from datetime import datetime, date
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Row
import json
import sys
import os
import pydeequ
import uuid

from pydeequ.suggestions import *
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.analyzers import *
      
search_path = sys.path
print(search_path)

spark = SparkSession.builder \
    .appName("Deequ-on-AWS-Lambda") \
    .master("local[*]") \
    .config("spark.jars.packages", "deequ-2.0.3-spark-3.3.jar")\
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.memory", "5g") \
    .config("spark.executor.memory", "5g") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

def lambda_handler(event, context):
    print(f'boto3 version: {boto3.__version__}')
    print(f'botocore version: {botocore.__version__}')
    df = spark.createDataFrame([
        Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
    ])
    print(df)
    return {"body": json.dumps(str(df.take(1)))}