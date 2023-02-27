import math
import dateutil.parser
import datetime
import time
import os
import logging
import json
import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def get_slots(intent_request):
    print(intent_request.keys())
    return intent_request['sessionState']['intent']['slots']


def lambda_handler(event, context):

    sqs = boto3.client('sqs')
    slots = (get_slots(event))
    
    session_id = event['sessionId']
    
    print(event)
    
    # if event['sessionAttributes'] is not None:
    #     session_attributes = event['sessionAttributes']
    # else:
    #     session_attributes = {}
    
    cuisine = slots['Cuisine']['value']['interpretedValue']
    location = slots['Location']['value']['interpretedValue']
    email = slots['Email']['value']['interpretedValue']
    date = slots['DiningDate']['value']['interpretedValue']
    time = slots['DiningTime']['value']['interpretedValue']
    ppl = slots['NumberOfPeople']['value']['interpretedValue']
    
    ##############################################
    ### check that the date and time are valid ###
    ##############################################
    
    target_date = datetime.date(int(date[0:4]), int(date[5:7]), int(date[8:]))
    current_date = datetime.date.today()
    
    if target_date < current_date:
        
        return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close",
                },
                "intent": {
                    "name": "DiningSuggestionsIntent",
                    "state": "Failed"
                }
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": "This request failed because your date requested is in the past. Please reload and retry the bot."
                }
            ]
        }

        # return {
        #     "dialogAction": {
        #         "type": "ElicitSlot",
        #         "messages": [
        #             {
        #                 "contentType": "PlainText",
        #                 "content": "Please enter a valid date that is not in the past."
        #             }
        #         ]
        #     },
        #   "intentName": "DiningSuggestionsIntent",
        #   "slots": {
        #         "Location": location,
        #         "DiningDate": date,
        #         "DiningTime": time,
        #         "Cuisine": cuisine,
        #         "People": ppl,
        #         "Email": email
        #     },
        #     "slotToElicit": "DiningDate"
        # }
        
        # return {
        #     "sessionState": {
        #         "dialogAction": {
        #             "type": "ElicitSlot",
        #         },
        #         "intent": {
        #             "name": "DiningSuggestionsIntent",
        #             "state": "Failed",
        #             "slotToElicit": "DiningDate"
        #         }
        #     },
        #     "messages": [
        #         {
        #             "contentType": "PlainText",
        #             "content": "Please enter a valid date that is not in the past."
        #         }
        #     ]
        # }

        
        # slots['DiningDate'] = None
        # return elicit_slot(
        #     {},
        #     "DiningSuggestionsIntent",
        #     slots,
        #     "DiningDate",
        #     {'contentType': 'PlainText', 'content': "Please choose a valid date that is not in the past"}
        # )
        
        # return {
        #     "dialogAction": {
        #         "type": "ElicitSlot",
        #         "message": {
        #           "contentType": "PlainText",
        #           "content": "Please choose a valid date that is not in the past"
        #         },
        #     "intentName": "DiningSuggestionsIntent",
        #     "slots": {
        #         "Location": location,
        #         "DiningDate": date,
        #         "DiningTime": time,
        #         "Cuisine": cuisine,
        #         "People": ppl,
        #         "Email": email
        #     },
        #     "slotToElicit" : "DiningDate"
        #     }
        # }
    
    elif target_date == current_date:
    
        # check if the of reservation is after the current time
        target_datetime = datetime.datetime(int(date[0:4]), int(date[5:7]), int(date[8:]), int(time[0:2]), int(time[3:]), int(0))  # replace with your target date
        current_datetime = datetime.datetime.now()-datetime.timedelta(hours=5)
        
        if target_datetime <= current_datetime:
            return {
                "sessionState": {
                    "dialogAction": {
                        "type": "Close",
                    },
                    "intent": {
                        "name": "DiningSuggestionsIntent",
                        "state": "Failed"
                    }
                },
                "messages": [
                    {
                        "contentType": "PlainText",
                        "content": "This request failed because you requested today for a time that is passed. Please reload and retry the bot."
                    }
                ]
            }
            
            
    #############################################
    ### check that the number of ppl is valid ###
    #############################################
    
    if int(ppl) <= 0:
        return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close",
                },
                "intent": {
                    "name": "DiningSuggestionsIntent",
                    "state": "Failed"
                }
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": "This request failed because you requested for less than 1 patron. Please reload and retry the bot."
                }
            ]
        }
        
    ##############################################
    ### check that the cuisine choice is valid ###
    ##############################################
    
    accepted_cuisines = ['japanese', 'chinese', 'italian', 'american', 'korean', 'mexican', 'british']
    
    if cuisine.lower() not in accepted_cuisines:
        return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close",
                },
                "intent": {
                    "name": "DiningSuggestionsIntent",
                    "state": "Failed"
                }
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": "This request failed because your cuisine is not supported. We currently support japanese, chinese, italian, american, korean, mexican, and british cuisines. Please reload and retry the bot."
                }
            ]
        }
    
    
    # Send SQS message
    
    sqs.send_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/012308860314/Queue1",
        MessageAttributes={
                'Cuisine': {
                    'DataType': 'String',
                    'StringValue': cuisine
                },
                'Location': {
                    'DataType': 'String',
                    'StringValue': location
                },
                'Email': {
                    'DataType': 'String',
                    'StringValue': email
                },
                'DiningDate': {
                    'DataType': 'String',
                    'StringValue': date
                },
                'DiningTime': {
                    'DataType': 'String',
                    'StringValue': time
                },
                'NumberOfPeople': {
                    'DataType': 'Number',
                    'StringValue': ppl
                }
        },
        MessageBody='Info'
    )

    return {
        "sessionState": {
            "dialogAction": {
                "type": "Close",
            },
            "intent": {
                "name": "DiningSuggestionsIntent",
                "state": "Fulfilled"
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": "We received your request and will email you suggestions!"
            }
        ]
    }