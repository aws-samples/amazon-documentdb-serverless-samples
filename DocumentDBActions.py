"""
This file provides the functionality of pulling the username and password from the secrets manager to connect to the DocumentDB that is the username, password and hostname.
For details on the setup of the documentdb and secrets manager. please refer to the blog (TODO : Post the link here)

Also note the variables that need to be modified to suit your environments
- Db port
- Pem file name.
- Secrets Name that you stored in secrets manager
- Region (Defaults to US East)
- Pem file name to refer to (relative to the execution environment)

"""
import json
import pymongo
import boto3
import base64
from botocore.exceptions import ClientError
import ConfigParser

# Define the Global variables

gdelt_config=ConfigParser.RawConfigParser()
gdelt_config.read('gdelt_parse_config.properties')

region_name = gdelt_config.get('DEFAULT','region_name')
document_db_port=gdelt_config.get('DEFAULT','document_db_port')
pem_locator=gdelt_config.get('DEFAULT','pem_locator')
username=gdelt_config.get('DEFAULT','docdb_username')
password=gdelt_config.get('DEFAULT','docdb_oassword')
docdb_host=gdelt_config.get('DEFAULT','docdb_host')

# Create a Secrets Manager client
db_client = pymongo.MongoClient('mongodb://'+username+':'+password+'@'+docdb_host+':'+document_db_port+'/?ssl=true&ssl_ca_certs='+pem_locator)


def queryTest():

    # Specify a holder for the db
    GDELT_DB_LOC = db_client['GDELT_DB']
    GDELT_DB_COLL = GDELT_DB_LOC['GDELT__COLL']
    
    queryString = GDELT_DB_COLL.find()
    
    # Assuming an Arrya has been returned print the first string and quit
    for rowStr in queryString:
        print(rowStr)
        break

def insertData(insert_list):
    #Create the GDELT Collection if not there
    #list databases
    #create GDELT Db
    #databases_list = db_client.list_database_names()
    #print(databases_list)
 
    GDELT_DB_LOC = db_client['GDELT_DB']
    GDELT_DB_COLL = GDELT_DB_LOC['GDELT__COLL']
    
    
    GDELT_DB_COLL.insert_many(insert_list)

   
def insertDataSingle(insert_list):
    #Create the GDELT Collection if not there
    #list databases
    #create GDELT Db
    #databases_list = db_client.list_database_names()
    #print(databases_list)
 
    GDELT_DB_LOC = db_client['GDELT_DB']
    GDELT_DB_COLL = GDELT_DB_LOC['GDELT__COLL']
    
    GDELT_DB_COLL.insert_many(insert_list)
    
    
def cleanupDb():
    #Create the GDELT Collection if not there
    #list databases
    #create GDELT Db
    #databases_list = db_client.list_database_names()
    #print(databases_list)
 
    GDELT_DB_LOC = db_client['GDELT_DB']
    GDELT_DB_COLL = GDELT_DB_LOC['GDELT__COLL']
    
    GDELT_DB_COLL.delete_many({})
    
queryTest()

#insertData()

#cleanupDb()


