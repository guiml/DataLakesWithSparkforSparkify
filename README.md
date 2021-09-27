# AWS Pipeline processing

## Running this script:

In order to run this script you will need to following requirements:
1) Create an EMR cluster within AWS
2) Upload the ETL file to the instance where the AWS EMR is running
3) Run the etl.py file using the spark-submit command

To run the step 1, execute the following code on CLI:

    aws emr create-cluster \
    --name emr_udacity \
    --use-default-roles \
    --release-label emr-5.28.0 \
    --instance-count 3 \
    --applications Name=Spark  \
    --ec2-attributes KeyName=[PRIVATE KEY FILE],SubnetId=[SUBNET NAME] \
    --instance-type m5.xlarge \
    --auto-terminate

Then take note of the cluster name. Go to the AWS Console in the EMR cluster created, edit the Security Group to accept SSH access.

In order to run this correctly, you will need a file *dl.cfg* with the following content:

    [KEY]
    KEY=INSERT_HERE_YOUR_AMAZON_KEY
    SECRET=INSERT HERE YOUR AMAZON KEY

Copy both dl.cfg and etl.py to the instance where the Spark session will run by running the following code on terminal (get the server address from the EMR Cluster page):

    scp -i [PRIVATE KEY FILE].pem etl.py hadoop@[SERVER ADDRESS]:/home/hadoop/
    scp -i [PRIVATE KEY FILE].pem dl.cfg hadoop@e[SERVER ADDRESS]:/home/hadoop/

Connect to the server, by typing the SSH command into the terminal:

    ssh -i [PRIVATE KEY FILE].pem hadoop@e[SERVER ADDRESS]

In the server, execute the etl.py code in the Spark environment:

    /usr/bin/spark-submit --master yarn etl.py

## Purpose of this database 

Sparify is a streaming startup that is growing its user base and database and wish to move their database to the cloud. They used to store their data in JSON files in their on-prem servers. The data was made available in S3 buckets in order to be transitioned into a Parquet Database.

This project is composed of an ETL pipeline that extracts data from S3, stages them in AWS EMR, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Files in the repository

The origin repository is comprised of two sets of files: 

**1) Song Dataset**
The first dataset is distributed among various JSON formated files containing metadata about a song and the artist of that song.

**2) Logs Dataset**
The second dataset consists of log files in JSON format with simulated app activity logs from the music streaming app.