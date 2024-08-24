#!/usr/bin/env bash

#Script used to push the image to ECR repository. Please ensure that the role associated with the instance has access to AWS ECR

# Check if AWS CLI is installed
aws --version > /dev/null 2>&1
if [[ $? -ne 0 ]]; then
  # Install AWS CLI
  curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
  unzip awscliv2.zip
  sudo ./aws/install
  aws configure
else
  echo "AWS CLI is already installed"
fi

echo "Starting the PUSH to AWS ECR...."

if [ $# -eq 0 ]
  then
    echo "Please provide the image name"
fi

Dockerimage=$1
FRAMEWORK=$2
Dockername="spark-on-lambda"

# Fetch the AWS account number
aws_account=$(aws sts get-caller-identity --query Account --output text)

if [ $? -ne 0 ]
then
    exit 255
fi

# Get the region defined in the current configuration (default to us-west-2 if none defined)
aws_region=$(aws configure get region)
aws_region=${region:-us-east-1}
reponame="${aws_account}.dkr.ecr.${aws_region}.amazonaws.com/${Dockername}:latest"

# Creates a repo if it does not exist
echo "Create or replace if repo does not exist...."
aws ecr describe-repositories --repository-names "${Dockername}" > /dev/null 2>&1

if [ $? -ne 0 ]
then
    aws ecr create-repository --repository-name "${Dockername}" > /dev/null
fi

# Get the AWS ECr login 
aws ecr get-login-password --region "${aws_region}" | docker login --username AWS --password-stdin "${aws_account}".dkr.ecr."${aws_region}".amazonaws.com

# Build the docker image and push to ECR
echo "Building the docker image"
docker build --build-arg FRAMEWORK=$FRAMEWORK -t ${Dockerimage} .


echo "Tagging the Docker image"
docker tag ${Dockerimage} ${reponame}


echo "Pushing the Docket image to AWS ECR"
docker push ${reponame}
