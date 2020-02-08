import __main__

from os import environ, listdir, path

from pyspark import SparkFiles
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.functions import when, isnull, col


def spark_start(master='local[*]', app_name='my_app', jars=[], files=[], spark_config={}):
    flag_repl = not(hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    if not (flag_repl or flag_debug):
        spark_builder = (
            SparkSession
            .builder
            .appName(app_name)
        )

    else:
        spark_builder = (
            SparkSession
            .builder
            .master(master)
            .appName(app_name)
        )

        spark_jars = ','.join(jars)
        spark_builder.config('spark.jars', spark_jars)

        spark_files = ','.join(files)
        spark_builder.config('spark.files', spark_files)

        for key, val in spark_config.items():
            spark_builder.config(key, val)

    ss = spark_builder.getOrCreate()

    return ss

def clean_data(spark_session, file_path):
    df = (
        spark_session
        .read
        .csv(file_path, header='true')
    )
    

    result = (
        df
        .withColumn(
            "tmp_address",
            when(
                isnull(col("subCategory")),
                col("id")
            ).otherwise(col("address"))
        )
        .withColumn(
            "id",
            when(
                isnull(col("subCategory")),
                col("address")
            ).otherwise(col("id"))
        )
        .withColumn(
            "details",
            when(
                isnull(col("subCategory")),
                col("originalId")
            ).otherwise(col("details"))
        )
        .withColumn(
            "reviews",
            when(
                isnull(col("subCategory")),
                col("polarity")
            ).otherwise(col("reviews"))
        )
        .withColumn(
            "polarity",
            when(
                isnull(col("subCategory")),
                None
            ).otherwise(col("polarity"))
        )
        .withColumn(
            "originalId",
            when(
                isnull(col("subCategory")),
                None
            ).otherwise(col("originalId"))
        )
        .withColumn(
            "tmp_category",
            when(
                isnull(col("subCategory")),
                col("lat")
            ).otherwise(col("category"))
        )
        .withColumn(
            "lat",
            when(
                isnull(col("subCategory")),
                col("location")
            ).otherwise(col("lat"))
        )
        .withColumn(
            "location",
            when(
                isnull(col("subCategory")),
                col("lng")
            ).otherwise(col("location"))
        )
        .withColumn(
            "lng",
            when(
                isnull(col("subCategory")),
                col("name")
            ).otherwise(col("lng"))
        )
        .withColumn(
            "name",
            when(
                isnull(col("subCategory")),
                col("category")
            ).otherwise(col("name"))
        )
    )

    result = result.drop(*["address", "category"])
    result = (
        result
        .withColumnRenamed("tmp_address", "address")
        .withColumnRenamed("tmp_category", "category")
    )
    
    return result

def write_to_checkins(df):
    url = "jdbc:postgresql://localhost:5432/hiddengems_db"
    table = "checkins"
    mode = "overwrite"
    properties = {
        "user": "postgres", 
        "password": "password", 
        "driver": "org.postgresql.Driver"
        }
    
    df.write.jdbc(url, table, mode, properties)