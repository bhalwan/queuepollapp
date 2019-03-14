import boto3
from boto3_type_annotations.sqs import Client
import time
import uuid
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import json

QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/334146420596/bhalwan-test-queue1.fifo'
FOLDER_PATH = "/home/ec2-user/pythonapp"
FILE_PATH = FOLDER_PATH+"/message_config.json"

class MyHandler(PatternMatchingEventHandler):

    patterns =["*/message_config.json"]

    def on_modified(self, event):
        with open(FILE_PATH) as f:
            data = json.load(f)
            for message in data["portfolios"]:
                write_message(message)


def main():
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path=FOLDER_PATH, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

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
