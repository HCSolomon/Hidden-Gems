from sparkhelpers import spark_start
from pyspark.sql import SparkSession
from spark_to_postgres_helpers import write_to_table, write_to_checkins

import os
import boto3
import time

def main():
    s3 = boto3.client('s3')
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
    sparkClassPath = os.getenv('SPARK_CLASSPATH', '~/.local/lib/python3.5/site-packages/pyspark/jars/postgresql-42.2.10.jar')
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    master = os.getenv('SPARK_MASTER_IP')

    spark_session = spark_start(master, 'hidden-gems', [sparkClassPath], [], {})
    sc = spark_session.sparkContext

    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    hadoop_conf.set('fs.s3a.access.key', aws_access_key_id)
    hadoop_conf.set('fs.s3a.secret.key', aws_secret_access_key)

    # bucket = input('Specify the name of the bucket to be processed:\n')
    # file_type = input('Type of files to be processed:\n')
    bucket = 'yelpreview-data'
    file_type = 'json'
    keys = [obj['Key'] for obj in s3.list_objects_v2(Bucket=bucket)['Contents'] if obj['Key'].split('.')[-1] == file_type]
    
    for key in keys:
        if (key != 'checkin.json'):
            file_path = 's3a://{}/{}'.format(bucket, key)
            write_to_table(spark_session, key.split('.')[0], file_path, ['attributes','hours'])
        else:
            write_to_checkins(spark_session, file_path)

if __name__ == "__main__":
    main()