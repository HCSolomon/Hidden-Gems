import __main__

from os import environ, listdir, path

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, isnull, col


def spark_start(master='local[*]', app_name='my_app', jar_packages=[], files=[], spark_config={}):
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

        spark_jars_packages = ','.join(jar_packages)
        spark_builder.config('spark.jars.packages', spark_jars_packages)

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
            "tmp_id",
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
            )
        )
    )
    result.select(result.tmp_address.alias("address"), result.tmp_id.alias("id")).show()
    result.where(isnull(col("subCategory"))).show()
    result.show()