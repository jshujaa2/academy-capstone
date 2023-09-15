from pathlib import Path
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrame, SQLContext
import pyspark.sql.functions as sf
from pyspark.sql.types import *

import json

import boto3

def get_secret():
    secret_name = "snowflake/capstone/login"
    region_name = "eu-west-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return get_secret_value_response

def read_data(path: Path):
    config = {
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.0,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.13.3" ,
        "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }

    conf = SparkConf().setAll(config.items())
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark.read.json(
        str(path)
    )

def clean(frame: DataFrame) -> DataFrame:
    columns_to_drop = ["coordinates", "date"]
    frame = (frame.select("*","coordinates.latitude", "coordinates.longitude","date.local", "date.utc")
                  .drop(*columns_to_drop)
                  .withColumn("local", sf.to_timestamp("local"))
                  .withColumn("utc", sf.to_timestamp("utc")))
    return frame

def write_data(frame: DataFrame):
    secret = get_secret()
    sfOptions = {
        "sfURL" : json.loads(secret['SecretString'])["URL"],
        "sfUser" : json.loads(secret['SecretString'])["USER_NAME"],
        "sfPassword" : json.loads(secret['SecretString'])["PASSWORD"],
        "sfDatabase" : json.loads(secret['SecretString'])["DATABASE"],
        "sfSchema" : "JORDY",
        "sfWarehouse" : json.loads(secret['SecretString'])["WAREHOUSE"]
    }

    (frame.write
         .format("snowflake")
         .options(**sfOptions)
         .option("dbtable", "capstone_jordy")
         .mode("overwrite")
         .save())

if __name__ == "__main__":
    # use relative paths, so that the location of this project on your system
    # won't mean editing paths
    path_to_exercises = Path(__file__).parents[1]
    resources_dir = path_to_exercises / "source"
    target_dir = path_to_exercises / "target"
    # Create the folder where the results of this script's ETL-pipeline will
    # be stored.
    target_dir.mkdir(exist_ok=True)

    # Extract
    frame = read_data('s3a://dataminded-academy-capstone-resources/raw/open_aq/')
    
    # Transform
    cleaned_frame = clean(frame)
    cleaned_frame.printSchema()
    cleaned_frame.show()
    # Load
    write_data(cleaned_frame)
