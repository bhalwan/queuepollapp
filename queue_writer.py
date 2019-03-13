import boto3
from boto3_type_annotations.sqs import Client
import time
import uuid

QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/334146420596/bhalwan-test-queue1.fifo'


def main():
    count: int = 0
    while True:
        message_body = "Portfolio" + str(count)
        write_message(message_body)
        count = count + 1
        # send a message every 10 seconds
        time.sleep(10)


def write_message(message_body: str):
    # get the number of messages on the queue
    sqs: Client = boto3.client('sqs', 'us-east-2')
    print("Sending Message " + message_body)
    deDuplicationId = str(uuid.uuid1().bytes)
    print(deDuplicationId)
    sqs.send_message(QueueUrl=QUEUE_URL,
                     MessageBody=message_body,
                     MessageGroupId="test",
                     MessageDeduplicationId=deDuplicationId)


if __name__ == '__main__':
    main()
