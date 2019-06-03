import json
import pymongo
import boto3
import base64
import configparser
import os
import ast
from botocore.exceptions import ClientError

secret_name = os.environ['secret_name']
region_name = os.environ['region']
document_db_port=os.environ['db_port']
pem_locator=os.environ['pem_locator']
    
session = boto3.session.Session()
client = session.client(service_name='secretsmanager', region_name=region_name)

get_secret_value_response = "null"
# GET THE SECRET
	
try:
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
except ClientError as e:
    raise e
	    
secret_data = json.loads(get_secret_value_response['SecretString'])

username = secret_data['username']
password = secret_data['password']
docdb_host=secret_data['host']

db_client = pymongo.MongoClient('mongodb://'+username+':'+password+'@'+docdb_host+':'+document_db_port+'/?ssl=true&ssl_ca_certs='+pem_locator)
  

def lambda_handler(event, context):
   
    httpmethod = event["httpMethod"]
    queryval=''
    #Support both get and post if the method invoked is get, then access the query parameters
    print("In Lambda Handler Function "+httpmethod+" Method")
    if httpmethod == "GET" :
        querybodyvaldict=event['multiValueQueryStringParameters']
        queryval = querybodyvaldict['dbquery'][0]
    else :
        querybodyval=ast.literal_eval(event['body'])
        queryval=querybodyval["dbquery"]
        
    print("Query type " + queryval)        
    querylower=queryval.lower()

    #A simple error message for usage, Just to get the body right    
    errorstring="Invalid Query, please pass 'total number of events', 'number of mentions for {keyword}, most talked event"
    return_val= {
                "isBase64Encoded": "true",
                "statusCode": 200,
                "headers": { "headerName": "headerValue"},
                "body": errorstring
            }
            
            
    try:
        GDELT_DB_LOC = db_client['GDELT_DB']
        GDELT_DB_COLL = GDELT_DB_LOC['GDELT__COLL']
        if querylower == "most talked event" :
            queryString = GDELT_DB_COLL.find_one(sort=[("NumMentions", -1)])
            topicval = queryString["SOURCEURL"]
            topicmention = queryString["NumMentions"]
            return_val['body'] = 'Most mentioned article ('+str(topicmention)+' Times) was '+topicval
        elif  querylower == "total number of events":
            queryString = GDELT_DB_COLL.find()
            totalcount = queryString.count()
            return_val['body'] = "Total Number of events for the day reported " + str(totalcount) 
        
        elif querylower.startswith("number of mentions") :
            searchstart = queryval.index('{')+1
            searchsend=queryval.index('}')
            searchstring = queryval[searchstart:searchsend]
            queryDict = ast.literal_eval("{\"SOURCEURL\": {\"$regex\": u\""+searchstring+"\"}}")
            queryString = GDELT_DB_COLL.find(queryDict)
            counttotal = queryString.count()
            
            return_val['body'] = searchstring+" was mentioned "+str(counttotal)+" times"
            
        return return_val
    except Exception as ex:
        # Send some context about this error to Lambda Logs
        print(ex)
        
  
