
# Real Time Twitter Streaming Data Pipeline

## Introduction

Social media platforms generate vast amounts of data every day, which can be used for various applications, including sentiment analysis, trend detection, and customer behavior analysis. Twitter is one of the most popular social media platforms, and its data is particularly valuable for real-time analysis. However, analyzing Twitter data can be challenging due to its dynamic and unstructured nature.

In this project, we have developed a Twitter streaming pipeline that allows for the ingestion, processing, and analysis of live Twitter data. The pipeline consists of several components, including Socket, Apache Spark, Hadoop Distributed File System (HDFS), and Apache Hive. The pipeline architecture is designed to handle large volumes of data in real-time and provide insights into Twitter user behavior.

## Project Overview

The aim of the project is:
- Ingest live Twitter data using Socket and Spark Streaming
- Process and transform the data into a format suitable for storage in HDFS
- Store the data in HDFS as Parquet files
- Create an external Hive table on top of the Parquet files
- Analyze and query the data using Hive and Spark SQL
- Create dimensions and fact tables using Hive and Spark SQL

## Architecture

The Twitter streaming pipeline consists of several components that work together to ingest, process, and analyze live Twitter data. The pipeline architecture is designed to handle large volumes of data in real-time and provide insights into Twitter user behavior.

The main components of the pipeline are:
1. Twitter Streaming API
2. Socket
3. Apache Spark
4. Hadoop Distributed File System (HDFS)
5. Apache Hive
6. Spark SQL

The pipeline is designed to work in a distributed environment, with each component running on a separate node or cluster.

## Getting Started

To set up and configure the pipeline, follow these steps:
1. Create a Twitter API developer account and obtain a bearer token
2. Set the bearer token as an environment variable
3. Configure the pipeline settings (e.g., HDFS location, Hive table schema)
4. Run the data collection system and Spark job

## Running the Project

1. Open two separate terminals and run the following commands in each of them:
   
   ```
   python3 /path/to/five_minutes_listener.py
   ```
   
   ```
   /opt/spark3/bin/spark-submit --master yarn --deploy-mode cluster /path/to/twitter_structured_stream.py
   ```
   
   Replace `/path/to` with the actual path to the Python script and Spark script respectively.

2. After running the first step, add the following command to a shell script file (e.g. `twitter.sh`):
   
   ```
   /opt/spark2/bin/spark-sql -f /path/to/spark_hive.sql
   ```
   
   Replace `/path/to` with the actual path to the `spark_hive.sql` file.

3. Add the shell script file to crontab to run it at specific intervals. For example, to run it every 10 minutes, add the following line to crontab:
   
   ```
   10 * * * * /path/to/twitter.sh
   ```
   
   Replace `/path/to` with the actual path to the `twitter.sh` file.

## Data Collection System

The data collection system is responsible for ingesting live Twitter data using the Twitter Streaming API and sending it over a socket connection to a listening client. The code is written in Python and uses various modules and functions to handle API requests and socket communication. The collected data is in JSON format.

## Landing Data Persistence

The landing data persistence phase is responsible for ingesting the JSON data from the socket connection, processing it using Spark Streaming, and storing it in HDFS as Parquet files. The data is partitioned by year, month, day, and hour. The Parquet files can be used to create an external Hive table.

## Data Analysis and Querying

Once the data is stored in HDFS as Parquet files, an external Hive table can be created on top of the files. This allows for querying and analyzing the data using Hive and Spark SQL. Dimension and fact tables can be created using SQL queries to perform more complex analysis.

## Conclusion

The Real Time Twitter Streaming Data Pipeline is a powerful tool for ingesting, processing, and analyzing live Twitter data. It can be used for various applications, including sentiment analysis, trend detection, and customer behavior analysis. By following the steps outlined in this README file, you can set up and configure the pipeline for your specific use case.
