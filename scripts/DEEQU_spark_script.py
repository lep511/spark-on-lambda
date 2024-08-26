import os
import pydeequ
import uuid

from pydeequ.suggestions import *
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.analyzers import *


from datetime import datetime

from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.functions import *

import json
import pandas as pd
"""
 Function that gets triggered when AWS Lambda is running.
 We are using the example from Redshift documentation
 https://docs.aws.amazon.com/redshift/latest/dg/spatial-tutorial.html#spatial-tutorial-test-data
 
 We are using PyDeequ library which uses Apache 2.0 license. Please refer to LICENSE.Apache.txt file for more details.

  Lambda General Configuration for above input file. Based on the input file size, the memory can be updated.
  Memory                 2048 MB
  Tmeout                 2 min
  Ephemeral storage      1024 MB

  Select the Lambda architecture (arm64 or x84_64) based on the your source machine where docker build have been executed
"""

def main():
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
 
    spark = SparkSession.builder \
        .appName("Spark-on-AWS-Lambda") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .enableHiveSupport().getOrCreate()

    # Reading the parquet file
    df = spark.read.parquet("s3a://aws-bigdata-blog/generated_synthetic_reviews/data/product_category=Electronics/")
    print(df.printSchema())

    analysisResult = AnalysisRunner(spark) \
                        .onData(df) \
                        .addAnalyzer(Size()) \
                        .addAnalyzer(Completeness("review_id")) \
                        .addAnalyzer(Distinctness("review_id")) \
                        .addAnalyzer(Mean("star_rating")) \
                        .addAnalyzer(Compliance("top star_rating", "star_rating >= 4.0")) \
                        .addAnalyzer(Correlation("total_votes", "star_rating")) \
                        .addAnalyzer(Correlation("total_votes", "helpful_votes")) \
                        .run()
                        
    analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
    pd.options.display.float_format = '{:,.7g}'.format
    print(analysisResult_df.show())
    analysisResult = AnalysisRunner(spark) \
                        .onData(df) \
                        .addAnalyzer(Compliance("after-1996 review_year", 
    "review_year >= 1996")) \
                        .addAnalyzer(Compliance("before-2017 review_year", 
    "review_year <= 2017")) \
                        .run()
    analysisResult_pd_df = AnalyzerContext.successMetricsAsDataFrame(spark,
    analysisResult, pandas=True)
    print(analysisResult_pd_df)

    analysisResult = AnalysisRunner(spark) \
                        .onData(df) \
                        .addAnalyzer(Compliance("range1996to2017 review_year",
    "review_year >= 1996 and review_year <= 2017")) \
                        .addAnalyzer(Compliance("values insight", 
    "insight == 'Y' or insight == 'N'")) \
                        .run()
    analysisResult_pd_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult, pandas=True)
    print(analysisResult_pd_df)
    
