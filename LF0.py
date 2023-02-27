import boto3
# Define the client to interact with Lex

client = boto3.client('lexv2-runtime')
def lambda_handler(event, context):
    msg_from_user = event['messages'][0]["unstructured"]["text"]

    #Can hard code in a response to test
    #msg_from_user = "Hello"
    
    # Initiate conversation with Lex
    response = client.recognize_text(botId='GDQPPWZRUY', botAliasId='54D3TVLFD5', localeId='en_US', sessionId='testuser', text=msg_from_user)

    print(response.get('messages', []))
    
    msg_from_lex = response.get('messages', [])[0]['content']
        
    return {
        'headers': {
            'Access-Control-Allow-Origin': '*'
        },
        'messages': [ {'type': "unstructured", 'unstructured': {'text': msg_from_lex }  } ]
    }