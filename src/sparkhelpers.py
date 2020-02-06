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
    result.where(isnull(col("subCategory"))).show()
    result.show()