import json
import sys
import spark_script
      
search_path = sys.path
print(search_path)

def lambda_handler(event, context):
    spark_script.main()
    return {"body": "ok"}
