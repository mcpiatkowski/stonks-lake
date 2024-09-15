import pandas as pd
import boto3
from io import StringIO


def handler(event, context):

    print(event)

    s3_event = event['Records'][0]['s3']
    bucket = s3_event['bucket']['name']
    key = s3_event['object']['key']

    s3 = boto3.client('s3')

    response = s3.get_object(Bucket=bucket, Key=key)

    file_content = response['Body'].read().decode('utf-8')

    df = pd.read_csv(StringIO(file_content))

    print(df.head())

    return {
        "status_code": 200,
        "body": event
    }