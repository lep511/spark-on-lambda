import boto3
import sys
import os
import subprocess
import logging
import json

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def lambda_handler(event, context):

    logger.info("******************Start AWS Lambda Handler************")

    # Run the spark-submit command on the local copy of teh script
    try:
        logger.info('Spark-Submitting the Spark script')
        subprocess.run(["spark-submit", "spark_script.py", "--event", json.dumps(event)], check=True, env=os.environ)
    except Exception as e :
        logger.error(f'Error Spark-Submit with exception: {e}')
        raise e
    else:
        logger.info(f'Script {input_script} successfully submitted')