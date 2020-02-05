import __main__

from os import environ, listdir, path

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
        .csv(file_path)
    )
    
    df.where(F.isnull(F.col("_c9"))).show()