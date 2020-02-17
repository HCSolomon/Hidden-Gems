import boto3

def main():
    s3 = boto3.client('s3')

    aws_access_key_id = input('Please enter your AWS Access Key ID:\n')
    aws_secret_access_key = input('Please enter your AWS Secret Access Key:\n')

    config = {
        'fs.s3n.awsAccessKeyId':aws_access_key_id,
        'fs.s3n.awsSecretAccessKey':aws_secret_access_key
    }

    bucket = input('Specify the name of the bucket to be processed:\n')
    file_type = input('Type of files to be processed:\n')

    keys = [obj['Key'] for obj in s3.list_objects_v2(Bucket=bucket)['Contents'] if obj['Key'].split('.')[-1] == file_type]
    
    files = ['s3n://{}/{}'.format(bucket, key) for key in keys]
    
    

if __name__ == "__main__":
    main()