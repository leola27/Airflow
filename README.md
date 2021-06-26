# Udacity Data Engineer Nanodegree project

## Data Pipelines with Apache Airflow

### Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

### Database schema design and ETL process
The source data resides in S3 in the form of JSON files and needs to be processed in Amazon Redshift. Thefore is one fact table, songplays, and a few dimension tables.The process takes data from S3, loads it into fact and dimension tables and performs data quality checks.

### Files in repository
Files consist of three directories. DAGS directory contains a dag for table creation, because this task is normally only ran once, and a dag for ETL. It also contains a script for creating tables
PLUGINS directory contains operatons performing ETL tasks, each of them is described below
REDSHIFT_CLUSTER_CREATION contains Python scripts for creating a redshift cluster, checking its status and deleting cluster once work si done. It also contains a configuration file. 


#### StageToRedshiftOperator
This operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

#### LoadFactOperator
The operator is expected to take as input a SQL statement and target database on which to run the query against.

#### LoadDimensionOperator
 Allows to switch between append-only and truncate-insert patterns, since it is often a requirment for dimension tables.

#### DataQualityOperator
The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. 

udac_etl_dag.py  calls above operators one by one in the order specified in the bottom of the script.

#### S3 bucket, Redshift cluster connection and AWS credentials 
S3 bucket is created via  AWS management console.Connections for aws_credentials (containing IAM role credentials) and redshift cluster are created in the Admin tab of Airflow.
