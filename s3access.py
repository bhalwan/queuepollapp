import boto3
import io
import pandas as pd
from boto3_type_annotations.s3 import Client, ServiceResource, Bucket
import argparse
from boto3_type_annotations.s3.waiter import BucketExists
from boto3_type_annotations.s3.paginator import ListObjectsV2

def main():
    readBucketsIntoDataFrame()

def readBucketsIntoDataFrame():
    s3: Client = boto3.client('s3')
    bucket = 'bhalwan-test-bucket'
    fileName = 'stock_prices.txt'
    response = s3.get_object(Bucket=bucket, Key=fileName)
    df = pd.read_csv(io.BytesIO(response['Body'].read()))
    print(df)

if __name__ == '__main__':
    main()
