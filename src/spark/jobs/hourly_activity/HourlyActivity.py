from pyspark import SparkFiles
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.functions import when, isnull, col, explode, split

import os

def analyze(ss, json_file):
    print('Calculating hourly activities')
    df = ss.read.json(json_file)
    try:
        df = df.select('*', explode(split('date', ', ')))
    except:
        print('** No "data" field found in HourlyActivity file input **')
        exit(-1)
    ip = os.getenv('POSTGRES_IP')
    url = "jdbc:postgresql://" + ip + "/hiddengems_db"
    table = "checkins"
    mode = "overwrite"
    properties = {
        "user": "postgres", 
        "password": "password", 
        "driver": "org.postgresql.Driver"
        }
    try:
        df = df.drop(col('date')).withColumnRenamed('col','date')
    except:
        print('** Warning: The file provided in HourlyActivity does not have the correct schema. **')
        exit(-1)
    
    df.write.jdbc(url, table, mode, properties)