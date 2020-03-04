import __main__

from os import environ, listdir, path

from pyspark import SparkFiles
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.functions import when, isnull, col, explode, split


def spark_start(master='local[*]', app_name='my_app', jars=[], files=[], spark_config={}):
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