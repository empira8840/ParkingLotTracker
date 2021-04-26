from __future__ import print_function

import boto3
from decimal import Decimal
import json
import urllib
from pprint import pprint
import time
import datetime

print('Loading function')

rekognition = boto3.client('rekognition')
client = boto3.client('sns')


# --------------- Function to call Rekognition APIs ------------------

def detect_labels(bucket, key):
    response = rekognition.detect_labels(Image={"S3Object": {"Bucket": bucket, "Name": key}})

    # Sample code to write response to DynamoDB table 'MyTable' with 'PK' as Primary Key.
    # Note: role used for executing this Lambda function should have write access to the table.
    # table = boto3.resource('dynamodb').Table('MyTable')
    # labels = [{'Confidence': Decimal(str(label_prediction['Confidence'])), 'Name': label_prediction['Name']} for label_prediction in response['Labels']]
    # table.put_item(Item={'PK': key, 'Labels': labels})
    return response


# def index_faces(bucket, key):
# Note: Collection has to be created upfront. Use CreateCollection API to create a collecion.
# rekognition.create_collection(CollectionId='BLUEPRINT_COLLECTION')
#    response = rekognition.index_faces(Image={"S3Object": {"Bucket": bucket, "Name": key}}, CollectionId="BLUEPRINT_COLLECTION")
#    return response

# ------------------Handler of dynamodb-------------------#

def put_log(name, message, time, count, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    table = dynamodb.Table('parkingTracker')
    respo = table.put_item(
        Item={
            'name': name,
            'message': message,
            'time': time,
            'count': count
        }
    )
    return respo


# --------------- Main handler ------------------
def lambda_handler(event, context):
    '''Demonstrates S3 trigger that uses
    Rekognition APIs to detect faces, labels and index faces in S3 Object.
    '''
    # print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    try:

        # Calls rekognition DetectLabels API to detect labels in S3 object
        response = detect_labels(bucket, key)

        timestamp = int(time.time())
        now = datetime.datetime.now()
        primaryK = "{}_{}_{}_{}".format(now.month, now.day,
                                        now.hour, now.minute)

        tosend = ""

        # for Label in response["Labels"]:
        #    print ('{0} - {1}%'.format(Label["Name";], Label["Confidence"]))
        #    tosend+= '{0} - {1}% '.format(Label["Name"], Label["Confidence"])

        # print (Label[&quot;Name&quot;] + Label[&quot;Confidence&quot;])

        for label in response['Labels']:
            if label['Name'] == 'Car':
                counter = str(len(label['Instances']))
                response = str(len(label['Instances'])) + " Cars detected in the Parking Lot " + "We have " + str(
                    8 - len(label['Instances'])) + " spots left "
        #      # TODO: write code...

        # Print response to console.
        print(response)

        # set the message to publish into the topic
        message = client.publish(
            TargetArn='arn:aws:sns:us-east-1:768495997260:image-rekognition-sns',
            Message=response,
            Subject='Parking Lot Tracker',
        )

        # call method put_log to insert log into dynamoDB
        rekognition_resp = put_log(key, response, primaryK, counter)

        # str(len(label['Instances']))

        return response
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e


# ----------------------------------------- #

# if __name__ == '__main__':
# rekognition_resp = put_log("YesGoIt",response)
print("Parking Log created:")
# pprint(rekognition_resp, sort_dicts=False)