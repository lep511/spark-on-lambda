export ACCOUNT_ID="488899227024"

curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com
aws ecr create-repository --repository-name spark-on-lambda --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE

# FRAMEWORK values: DEEQU, ICEBERG, REDSHIFT, SNOWFLAKE, DELTA, HUDI
export FRAMEWORK="ICEBERG"
docker build --build-arg FRAMEWORK=$FRAMEWORK -t lambda-pyspark .
bash push_to_ecr.sh lambda-pyspark

docker tag  lambda-pyspark:latest $ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/spark-on-lambda:latest
docker push $ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/spark-on-lambda


docker run -p 9000:8080 lambda-pyspark:latest
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"payload":"hello world!"}'
