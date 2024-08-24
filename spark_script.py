import os
import pydeequ
import uuid

from pydeequ.suggestions import *
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.analyzers import *


from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

"""
 Function that gets triggered when AWS Lambda is running.
 We are using the example from Redshift documentation
 https://docs.aws.amazon.com/redshift/latest/dg/spatial-tutorial.html#spatial-tutorial-test-data
 
 We are using PyDeequ library which uses Apache 2.0 license. Please refer to LICENSE.Apache.txt file for more details.

  Add below parameters in the lambda function Environment Variables
  SCRIPT_BUCKET         BUCKET WHERE YOU SAVE THIS SCRIPT
  SPARK_SCRIPT          THE SCRIPT NAME AND PATH
  INPUT_PATH            s3a://redshift-downloads/spatial-data/accommodations.csv
  OUTPUT_PATH           THE PATH WHERE THE VERIFICATION RESULTS AND METRICS WILL BE STORED

  Lambda General Configuration for above input file. Based on the input file size, the memory can be updated.
  Memory                 2048 MB
  Tmeout                 2 min
  Ephemeral storage      1024 MB

  Select the Lambda architecture (arm64 or x84_64) based on the your source machine where docker build have been executed
"""

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

def main():
    input_path = "accommodations.csv"
    output_path = "./"

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

    # Reading the csv file form input_path
    dataset = spark.read.option('header', 'true').option("delimiter", ";").csv(input_path)

    print("Schema of input file:")
    dataset.printSchema()

    analysisResult = AnalysisRunner(spark) \
                    .onData(dataset) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("host_name")) \
                    .addAnalyzer(ApproxCountDistinct("neighbourhood")) \
                    .addAnalyzer(Mean("price")) \
                    .run()

    analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
    print("Showing AnalysisResults:")
    analysisResult_df.show()

    check = Check(spark, CheckLevel.Warning, "Accomodations")

    checkResult = VerificationSuite(spark) \
        .onData(dataset) \
        .addCheck(
            check.hasSize(lambda x: x >= 22248) \
            .isComplete("name")  \
            .isUnique("id")  \
            .isComplete("host_name")  \
            .isComplete("neighbourhood")  \
            .isComplete("price")  \
            .isNonNegative("price")) \
        .run()


    print("Showing VerificationResults:")
    checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
    checkResult_df.show()

    checkResult_df.repartition(1).write.mode('overwrite').csv(output_path+"/verification-results/", sep=',')

    print("Showing VerificationResults metrics:")
    checkResult_df = VerificationResult.successMetricsAsDataFrame(spark, checkResult)
    checkResult_df.show()

    checkResult_df.repartition(1).write.mode('overwrite').csv(output_path+"/verification-results-metrics/", sep=',')

    spark.sparkContext._gateway.shutdown_callback_server()
    spark.stop()
