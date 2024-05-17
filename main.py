import requests
import boto3
import json
import random, string
from multiprocessing import Pool
import logging
import time
s3_client = boto3.client("s3",
    region_name="us-east-1",#os.environ.get('AWS_DEFAULT_REGION'),
    endpoint_url="http://localhost.localstack.cloud:4566",#os.environ.get('AWS_ENDPOINT'),
    aws_access_key_id="test", #os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key="test")#os.environ.get('AWS_SECRET_ACCESS_KEY'))

s3 = boto3.resource("s3",
    region_name="us-east-1",#os.environ.get('AWS_DEFAULT_REGION'),
    endpoint_url="http://localhost.localstack.cloud:4566",#os.environ.get('AWS_ENDPOINT'),
    aws_access_key_id="test", #os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key="test")#os.environ.get('AWS_SECRET_ACCESS_KEY'))
bucket_name="checkpoint-s3"

checkpoint_url="http://localhost:8081/api/add_message"
#This create a message with known file name to be upload to s3
def test_upload_defult_message():
    defult_message={"token":"notsecured","data":{"email_subject":"default-subject","email_sender":"sender1","email_content":"content","email_timestream":"123"}}
    defult_message_message_md5="59a1e514a552c9de6b32109263a5721f"
    res=requests.post(url=checkpoint_url, json=defult_message)
    if res.status_code != 200:
        return False
    else:
        return True

#This function try to uplad a message and return status code
def upload_message(message):
    res=requests.post(url=checkpoint_url, json=message)
    return res.status_code

#create random string
def randomword(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

#This function upload a number of message in multiproccess
def test_upload_multi_message_mp(number_of_messages=5,number_of_processes=5):
    defult_message={"token":"notsecured","data":{"email_subject":"default-subject","email_sender":"sender1","email_content":"content","email_timestream":"123"}}
    messages=[]
    counter=0
    while counter < number_of_messages:
        temp_message=defult_message.copy()
        temp_message["data"]["email_subject"]=str(temp_message["data"]["email_subject"]+randomword(10))
        messages.append(temp_message)
        counter+=1
    with Pool(number_of_processes) as p:
        responses=p.map(upload_message,messages)
    for response in responses:
        if response != 200:
            return False
    return True
#This function tests
def test_upload_bad_token():
    bad_token_message={"token":"bad_token","data":{"email_subject":"default-subject","email_sender":"sender1","email_content":"content","email_timestream":"123"}}
    if upload_message(bad_token_message) == 200:
        return False
    return True

def test_upload_bad_data():
    bad_data_message={"token":"notdecured","data":{"ema_subject":"default-subject","email_sender":"sender1","email_content":"content","email_timestream":"123"}}
    if upload_message(bad_data_message) == 200:
        return False
    return True

def test_upload_very_big_message(message_size):
    big_message={"token":"notsecured","data":{"email_subject":"big-subject","email_sender":"sender1","email_content":"content","email_timestream":"123"}}
    big_message["data"]["content"]=randomword(message_size)
    res=upload_message(big_message)
    if res != 200:
        return False
    return True

def get_number_of_s3_messages():
    counter=0
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.all():
        counter+=1
    print(counter)
    return counter

 
def test():
    pass

def tests():
    # tests for checkpoint-ms-1 
    logging.info("starting tests")
    logging.info("get number of messages s3 before tests starts")
    initial_s3_messages_num=get_number_of_s3_messages()
    number_of_messages_sent=0
    logging.info("test_upload_defult_message")
    assert test_upload_defult_message()
    #number_of_messages_sent+=1 bug in tests because usually the message is in s3 
    logging.info("test_bad_token")
    assert test_upload_bad_token()
    logging.info("test bad data")
    assert test_upload_bad_data()
    logging.info("test_upload_multi_message_mp")
    assert test_upload_multi_message_mp(5,5)
    number_of_messages_sent+=5
    #stress test for checkpoint-ms-1 
    logging.info("test_upload_multi_message_mp large")
    assert test_upload_multi_message_mp(10,5)
    number_of_messages_sent+=10
    logging.info("test_upload_very_big_message")
    assert test_upload_very_big_message(10000)
    number_of_messages_sent+=1
    # tests for checkpoint-ms-2
    logging.info("starting tests for checkpoint-ms-2")
    logging.info("sleeping for 60 seconds checkpoint-ms-2 to complete handle messages")
    time.sleep(60)
    excepted_num_of_s3_messages=initial_s3_messages_num+number_of_messages_sent
    logging.info("tests all messages lands in s3")
    assert excepted_num_of_s3_messages<get_number_of_s3_messages()

    
     


    
    






#print(bucket)

"""
bucket = s3.Bucket(bucket_name)
# Iterates through all the objects, doing the pagination for you. Each obj
# is an ObjectSummary, so it doesn't contain the body. You'll need to call
# get to get the whole body.
counter=0
for obj in bucket.objects.all():
    #print(obj)
    counter+=1
    key = obj.key
    print(obj.key)
    body = json.loads(obj.get()['Body'].read())
print(counter)
   # print(body)
"""
if __name__ == '__main__':
    logging.basicConfig(level = logging.INFO)
    tests()

