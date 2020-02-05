from os import environ, listdir, path

from pyspark import SparkFiles
from pyspark.sql import SparkSession

class SparkDriver:
    def __init__(self, master='local[*]', app_name='my_app', jar_packages=[], files=[], spark_config={}):
        self.master = master
        self.app_name = app_name
        self.jar_packages = jar_packages
        self.files = files
        self.spark_config = spark_config

    def start():
        flag_repl = not(hasattr(__main__, '__file__'))
        flag_debug = 'DEBUG' in environ.keys()

        if not (flag_repl or flag_debug):
            spark_builder = (
                SparkSession
                .builder
                .appName(self.app_name)
            )

        else:
            spark_builder = (
                SparkSession
                .builder
                .master(self.master)
                .appName(self.app_name)
            )

            spark_jars_packages = ','.join(self.jar_packages)
            spark_builder.config('spark.jars.packages', spark_jars_packages)

            spark_files = ','.join(self.files)
            spark_builder.config('spark.files', spark_files)

            for key, val in self.spark_config.items():
                spark_builder.config(key, val)

            ss = spark_builder.getOrCreate()

        return ss
            
            