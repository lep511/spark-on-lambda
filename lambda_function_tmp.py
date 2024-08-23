import boto3
import botocore
from datetime import datetime, date
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Row
import json
import sys
      
search_path = sys.path
print(search_path)

spark = SparkSession.builder.getOrCreate()

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