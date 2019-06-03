"""
Parses the raw data from the GDELT site for the current date and loads it into the DocumentDB collection.
This will create a folder gdelt_data to create the files requried to insert the data into the Documnent DB
Note : Its a pre-requisit that the document db has been created.
"""

import urllib2
import zipfile
import os
import datetime
import ConfigParser
import ast
import pymongo
from ast import literal_eval


#load the config file note the file name should be gdelt_parse_config.properties and has to follow the congi file format...
gdelt_config=ConfigParser.RawConfigParser()
gdelt_config.read('gdelt_parse_config.properties')

#gdelt_file = '20190426';
gdelt_file = gdelt_config.get('DEFAULT','gdelt_load_date')
#The fields that should be treated as numeric.
gdelt_numfields=ast.literal_eval(gdelt_config.get("DEFAULT",'gdelt_numeric_fields'))
region_name = gdelt_config.get('DEFAULT','region_name')
document_db_port=gdelt_config.get('DEFAULT','document_db_port')
pem_locator=gdelt_config.get('DEFAULT','pem_locator')
username=gdelt_config.get('DEFAULT','docdb_username')
password=gdelt_config.get('DEFAULT','docdb_oassword')
docdb_host=gdelt_config.get('DEFAULT','docdb_host')


# Create a Secrets Manager client
db_client = pymongo.MongoClient('mongodb://'+username+':'+password+'@'+docdb_host+':'+document_db_port+'/?ssl=true&ssl_ca_certs='+pem_locator)
GDELT_DB_LOC = db_client['GDELT_DB']
GDELT_DB_COLL = GDELT_DB_LOC['GDELT__COLL']

#The fields as published by the GDELT team
GDELT_HEADER = ["GLOBALEVENTID","SQLDATE","MonthYear","Year","FractionDate","Actor1Code","Actor1Name","Actor1CountryCode","Actor1KnownGroupCode","Actor1EthnicCode","Actor1Religion1Code","Actor1Religion2Code","Actor1Type1Code","Actor1Type2Code","Actor1Type3Code","Actor2Code","Actor2Name","Actor2CountryCode","Actor2KnownGroupCode","Actor2EthnicCode","Actor2Religion1Code","Actor2Religion2Code","Actor2Type1Code","Actor2Type2Code","Actor2Type3Code","IsRootEvent","EventCode","EventBaseCode","EventRootCode","QuadClass","GoldsteinScale","NumMentions","NumSources","NumArticles","AvgTone","Actor1Geo_Type","Actor1Geo_FullName","Actor1Geo_CountryCode","Actor1Geo_ADM1Code","Actor1Geo_Lat","Actor1Geo_Long","Actor1Geo_FeatureID","Actor2Geo_Type","Actor2Geo_FullName","Actor2Geo_CountryCode","Actor2Geo_ADM1Code","Actor2Geo_Lat","Actor2Geo_Long","Actor2Geo_FeatureID","ActionGeo_Type","ActionGeo_FullName","ActionGeo_CountryCode","ActionGeo_ADM1Code","ActionGeo_Lat","ActionGeo_Long","ActionGeo_FeatureID","DATEADDED","SOURCEURL"]
GDELT_DATA_LENGTH = 58;

#pull a file from S3 and store it locally - just pulling one day data file
url_link = "http://data.gdeltproject.org/events/"+gdelt_file+".export.CSV.zip"

#Creater a working directory

path = os.getcwd()
working_dir = path+'/gdelt_data'

print('Current Working Directory '+path)

#File download and convert to CSV
#create if not already exists...assume its not existing 
if not os.path.isdir(working_dir):
    workind_dir_path = os.mkdir(working_dir)
    
#remove files created before (makde changed to avoid downloading if you need)
try:
    files = [ f for f in os.listdir(working_dir) ]
    for f in files:
        os.remove(os.path.join(working_dir, f))
except Exception:
    print("Error deleting files, may be empty")


gdelt_file_response = urllib2.urlopen(url_link)
response_handler = open(working_dir+'/GDELT_'+gdelt_file+'.zip', 'w')
response_handler.write(gdelt_file_response.read())
response_handler.close()
gdelt_file_response.close()


print("DOWNLOADED...Uncompressing")


zipfile_helper = zipfile.ZipFile(working_dir+'/GDELT_'+gdelt_file+'.zip', 'r')
zipfile_helper.extractall(working_dir)
zipfile_helper.close()

print("Uncompress completed..process the CSV file to create the document data to insert (100) documents at a time")


#Try opening the file for loading into DocumentDB
try:
    csvfile =   open (working_dir+'/'+gdelt_file+'.export.CSV','r')
    document_insert=[]
    totalcount = 0
    for line in csvfile:
        line_data = line.split('\t')
        json_list =[]
        for i in range(GDELT_DATA_LENGTH):
            if GDELT_HEADER[i] not in gdelt_numfields:
                key_val = "\""+GDELT_HEADER[i]+"\":\""+line_data[i]+"\""
            else:
                key_val = "\""+GDELT_HEADER[i]+"\":"+line_data[i]
            if i < 58-1 : 
                json_list.append(key_val+',')
            else:
                json_list.append(key_val.replace('\n',''))
                
        jsonstring="".join(json_list)
        pdict = python_dict = literal_eval("{"+jsonstring+'}')
        document_insert.append(python_dict)
        
        #print("{"+jsonstring+'}')
        
        #print(document_insert)
        #totalcount+=100
        list_length = len(document_insert)
        #break
        if list_length == 1000:
            GDELT_DB_COLL.insert_many(document_insert)
            #print("REACHED HUNDRED")
            del document_insert[:]
            #print(len(document_insert))
        
        
        #print(document_insert)
        #json.dump(line, jsonfile)
        #jsonfile.write('\n')
    print(totalcount)
except Exception as e: 
    print(e)



    
    
    
    