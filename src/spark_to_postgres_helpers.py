from pyspark import SparkFiles
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.functions import when, isnull, col, explode, split

def write_to_checkins(ss, file_path):
    df = ss.read.json(file_path)
    cols = set(df.columns)

    if (cols != set(['business_id', 'date'])):
        print("** Warning: The file provided in write_to_checkins does not have the correct schema. **")
        return

    url = "jdbc:postgresql://localhost:5432/hiddengems_db"
    table = "checkins"
    mode = "overwrite"
    properties = {
        "user": "postgres", 
        "password": "password", 
        "driver": "org.postgresql.Driver"
        }

    df = df.select('*', explode(split('date', ', ')))
    df = df.drop(col('date')).withColumnRenamed('col','date')
    
    df.write.jdbc(url, table, mode, properties)