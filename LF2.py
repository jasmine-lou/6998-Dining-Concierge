import json
import boto3
import os
import random
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

sqs = boto3.client("sqs", region_name="us-east-1")
ses = boto3.client('ses')
dynamodb = boto3.resource('dynamodb')
es = boto3.client('es')

sqs_url = "https://sqs.us-east-1.amazonaws.com/012308860314/Queue1"
ses_arn = "arn:aws:ses:us-east-1:012308860314:identity/ebr2138@columbia.edu"
es_index = "restaurants"
dynamodb_table = "yelp-restaurants"
es_url = "search-restaurants-kwdic3ncbdbc2lbc3stnxfpivu.us-east-1.es.amazonaws.com"
region = 'us-east-1'

def lambda_handler(event, context):
    credentials = boto3.Session().get_credentials()
    
    response = sqs.receive_message(
        QueueUrl=sqs_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=15
    )
    
    if 'Messages' in response:
        sqs_message = response['Messages'][0]
        message = sqs_message["MessageAttributes"]
        cuisine = message["Cuisine"]["StringValue"]
        number_of_people = message["NumberOfPeople"]["StringValue"]
        date = message["DiningDate"]["StringValue"]
        time = message["DiningTime"]["StringValue"]
        email = message["Email"]["StringValue"]
                
        sqs.delete_message(
            QueueUrl=sqs_url,
            ReceiptHandle=sqs_message['ReceiptHandle']
        )
        
        service = 'es'
        credentials = boto3.Session().get_credentials()

        #Query is randomized so that restaurant recommendations are not always the same
        q = {"query": {
                "function_score": {
                'query': {'match': {'cuisine': cuisine}},
                "random_score": {}
                }
            }}
        
        #Can use this code to get same un-random restaurant recommendations each time
        #q = {'query': {'match': {'cuisine': cuisine}}}
        
        client = OpenSearch(
            hosts=[{
                'host': es_url,
                'port': 443
            }],
            http_auth=get_awsauth('us-east-1', 'es'),
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection)
            
        es_response = client.search(index="restaurants", body=q)
        
        restaurant_ids = [hit["_source"]["RestaurantID"] for hit in es_response['hits']['hits']]
        if len(restaurant_ids) <= 0:
            return "I could not find any restaurants that fit this description, so sorry!"
        message = "Here are some " + cuisine + " restaurant suggestions for " + number_of_people +" people on " + date + " at " + time + ": "   

        # Query DynamoDB to get more information about the restaurants
        restaurants = []
        table = dynamodb.Table(dynamodb_table)
        for restaurant_id in restaurant_ids:
            print(restaurant_id)
            restaurant = table.get_item(
                Key={
                    'id': restaurant_id,
                }
            )
            restaurants.append(restaurant['Item'])
                 
        count = 1
        for restaurant in restaurants:
            message += "\n" + str(count) + ": " + restaurant["name"] + " with rating " + restaurant["rating"] + " at " + restaurant["display_address"]
            count +=1
    
        subject = 'Your Restaurant Recommendations!'
        body = message
        sender = "ebr2138@columbia.edu"
        recipients = [email]
    
        emailContent = {
            'Subject': {'Data': subject},
            'Body': {'Text': {'Data': body}},
            'FromEmailAddress': sender,
            'To': [{'EmailAddress': r} for r in recipients]
        }
        
        sesClient = boto3.client('ses', region_name= 'us-east-1')
        
        response = ses.send_email(
            Source=sender,
            Destination={
                "ToAddresses": recipients
            },
            Message={
                "Subject": {
                    "Data": subject
                },
                "Body": {
                    "Text": {
                        "Data": body
                    }
                }
            }
        )

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
