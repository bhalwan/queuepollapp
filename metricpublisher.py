import boto3
from boto3_type_annotations.sqs import Client
from boto3_type_annotations.cloudwatch import Client as clc
import time

QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/334146420596/bhalwan-test-queue1.fifo'


def main():
    while True:
        message_count = get_pending_message_count()
        publish_metric(message_count)
        time.sleep(5)


def get_pending_message_count() -> int:
    # get the number of messages on the queue
    sqs: Client = boto3.client('sqs', 'us-east-2')
    attributes = sqs.get_queue_attributes(QueueUrl=QUEUE_URL, AttributeNames=['ApproximateNumberOfMessages'])
    numMessages = attributes['Attributes']['ApproximateNumberOfMessages']
    return int(numMessages)


def publish_metric(message_count: int):
    #get a handle to cloudwatch
    cloudwatchClient: clc = boto3.client('cloudwatch', 'us-east-2')

    # publish metric
    print("Num Messages " + str(message_count))
    cloudwatchClient.put_metric_data(Namespace='QueuePollerNamespace',
                                            MetricData=[{'MetricName': 'AverageQueueSize', 'Unit': 'None', 'Value': message_count}])



if __name__ == '__main__':
    main()
