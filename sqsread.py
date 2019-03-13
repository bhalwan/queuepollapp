import boto3
import time
import urllib.request
import sys

def main():
    #listQueues()
    readMessageFromQueue('	https://sqs.us-east-2.amazonaws.com/334146420596/bhalwan-test-queue1.fifo')

def listQueues():
    # Create SQS client
    sqs = boto3.client('sqs', 'us-east-2')

    # List SQS queues
    response = sqs.list_queues()

    print(response)


def write_report(message: str):
    #get instance name
    instanceid = urllib.request.urlopen('http://169.254.169.254/latest/meta-data/instance-id').read().decode()
    s3 = boto3.resource('s3')
    object = s3.Object('queue-report', instanceid+"-"+message+".txt")
    object.put(Body=message)


def readMessageFromQueue(queue_url):
    sqs = boto3.client('sqs', 'us-east-2')

    while True:
        try:
            print("Polling Queue...")
            # Receive message from SQS queue
            messages = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=[
                    'SentTimestamp'
                ],
                MaxNumberOfMessages=1,
                MessageAttributeNames=[
                    'All'
                ],
                VisibilityTimeout=30,
                WaitTimeSeconds=0
            )

            if 'Messages' in messages:  # when the queue is exhausted, the response dict contains no 'Messages' key
                for message in messages['Messages']:  # 'Messages' is a list
                    # process the messages
                    messageBody = message['Body']
                    print(messageBody)
                    write_report(messageBody)
                    # next, we delete the message from the queue so no one else will process it again
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
            else:
                print('Queue is now empty')
            time.sleep(90)
        except:
            print("Unexpected error:", sys.exc_info()[0])

if __name__ == '__main__':
    main()
