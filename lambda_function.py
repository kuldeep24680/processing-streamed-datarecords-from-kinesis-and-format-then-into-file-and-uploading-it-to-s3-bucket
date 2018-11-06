from __future__ import print_function
import json
import base64
from io import BytesIO
import boto3
import ast
def lambda_handler(event, context):
    for record in event['Records']:
       #Kinesis data is base64 encoded so decode here
       s3 = boto3.client('s3')
       payload=base64.b64decode(record["kinesis"]["data"])
       print("Decoded payload: " + str(payload))
      
       metadata_check1 = payload.decode("utf-8")
       metadata_check = ast.literal_eval(metadata_check1)
       
       print(metadata_check)
       
       if not metadata_check.get('metadata',{}).get('source',{}) or not metadata_check.get('metadata',{}).get('retention',{})  or not metadata_check.get('metadata',{}).get('business',{}) or not metadata_check.get('metadata',{}).get('usecase',{}) or not metadata_check.get('metadata',{}).get('ingestion_owner',{}) or not metadata_check.get('metadata',{}).get('level1_ownership',{}) or not metadata_check.get('metadata',{}).get('level2_ownership',{}) or not metadata_check.get('metadata',{}).get('confidentiality',{})  or not metadata_check.get('metadata',{}).get('quality',{})  or not metadata_check.get('metadata',{}).get('refresh_velocity',{}):
           
           data=byte(payload.decode("utf-8"))
           url= metadata_check["metadata"]["url"]
           prefix=""
           url=url.split('/')
           n=len(url)
           for i in range(1,n-1):
            prefix=prefix+url[i]+'/'
           prefix=prefix+url[n-1]
           s3.Bucket('onepurchase-test-s3-quarantinebucket').put_object(Key=prefix, Body=data, ServerSideEncryption="AES256")
           print("Uploaded to Quarantine Bucket due to missing metadata tags")
    
           
           
       elif metadata_check["metadata"]["source"]=='' or metadata_check["metadata"]["retention"]=='' or metadata_check["metadata"]["business"]==''  or metadata_check["metadata"]["usecase"]==''  or metadata_check["metadata"]["ingestion_owner"]==''  or metadata_check["metadata"]["expiration"]==''  or metadata_check["metadata"]["level1_ownership"]==''  or metadata_check["metadata"]["level2_ownership"]==''  or metadata_check["metadata"]["confidentiality"]==''  or metadata_check["metadata"]["quality"]=='' or metadata_check["metadata"]["refresh_velocity"]=='':
                       
            source=metadata_check["metadata"]["source"]
            retention= metadata_check["metadata"]["retention"]
            business=metadata_check["metadata"]["business"]
            usecase=metadata_check["metadata"]["usecase"]
            ingestion_owner=metadata_check["metadata"]["ingestion_owner"]
            expiration=metadata_check["metadata"]["expiration"]
            level1_ownership=metadata_check["metadata"]["level1_ownership"]
            level2_ownership=metadata_check["metadata"]["level2_ownership"]
            confidentiality=metadata_check["metadata"]["confidentiality"]
            quality=metadata_check["metadata"]["quality"]
            refresh_velocity=metadata_check["metadata"]["refresh_velocity"]
            
            print("testing for source: "+ metadata_check["metadata"]["source"])
            print("testing for prop: "+ str(metadata_check["prop"]))
            print("testing for timestamp: "+ str(metadata_check["timestamp"]))
            print("testing for retention: "+ metadata_check["metadata"]["retention"])
            print("testing for business: "+ metadata_check["metadata"]["business"])
            print("testing for data: "+ str(metadata_check["data"]))
            print("testing for usecase: "+ metadata_check["metadata"]["usecase"])
            print("testing for ingestion owner: "+ metadata_check["metadata"]["ingestion_owner"])
            print("testing for level1_ownership: "+ metadata_check["metadata"]["level1_ownership"])
            print("testing for level2_ownership: "+ metadata_check["metadata"]["level2_ownership"])
            print("testing for confidentiality: "+ metadata_check["metadata"]["confidentiality"])
            print("testing for expiration: "+ metadata_check["metadata"]["expiration"])
            print("testing for URL: "+ metadata_check["metadata"]["url"])
            print("testing for quality: "+ metadata_check["metadata"]["quality"])
            print("testing for refresh_velocity: "+ metadata_check["metadata"]["refresh_velocity"])
            
            
            
            
            data= BytesIO(metadata_check["data"].encode())
            url= metadata_check["metadata"]["url"]
            prefix=""
            url=url.split('/')
            n=len(url)
            for i in range(1,n-1):
                prefix=prefix+url[i]+'/'
            prefix=prefix+url[n-1]
            
          
            s3.put_object(Bucket='onepurchase-test-s3-quarantinebucket',Key=prefix, Body=data, ServerSideEncryption='AES256',Metadata={
        'retention': retention,
        'usecase': usecase,
        'quality': quality,
        'refresh_velocity': refresh_velocity,
        'source': source,
        'ingestion_owner': ingestion_owner,
        'confidentiality': confidentiality,
        'expiration': expiration,
        'level1_ownership': level1_ownership,
        'level2_ownership': level2_ownership,
        'url': 'onepurchase-test-s3-quarantinebucket/'+prefix
            })
            print("Uploaded to Quarantine Bucket due to missing metadata values")
           
       else:
           
            source=metadata_check["metadata"]["source"]
            retention= metadata_check["metadata"]["retention"]
            business=metadata_check["metadata"]["business"]
            usecase=metadata_check["metadata"]["usecase"]
            ingestion_owner=metadata_check["metadata"]["ingestion_owner"]
            expiration=metadata_check["metadata"]["expiration"]
            level1_ownership=metadata_check["metadata"]["level1_ownership"]
            level2_ownership=metadata_check["metadata"]["level2_ownership"]
            confidentiality=metadata_check["metadata"]["confidentiality"]
            quality=metadata_check["metadata"]["quality"]
            refresh_velocity=metadata_check["metadata"]["refresh_velocity"]
            
            print("testing for source: "+ metadata_check["metadata"]["source"])
            print("testing for prop: "+ str(metadata_check["prop"]))
            print("testing for timestamp: "+ str(metadata_check["timestamp"]))
            print("testing for retention: "+ metadata_check["metadata"]["retention"])
            print("testing for business: "+ metadata_check["metadata"]["business"])
            print("testing for data: "+ str(metadata_check["data"]))
            print("testing for usecase: "+ metadata_check["metadata"]["usecase"])
            print("testing for ingestion owner: "+ metadata_check["metadata"]["ingestion_owner"])
            print("testing for level1_ownership: "+ metadata_check["metadata"]["level1_ownership"])
            print("testing for level2_ownership: "+ metadata_check["metadata"]["level2_ownership"])
            print("testing for confidentiality: "+ metadata_check["metadata"]["confidentiality"])
            print("testing for expiration: "+ metadata_check["metadata"]["expiration"])
            print("testing for URL: "+ metadata_check["metadata"]["url"])
            print("testing for quality: "+ metadata_check["metadata"]["quality"])
            print("testing for refresh_velocity: "+ metadata_check["metadata"]["refresh_velocity"])
            
            
            
            
            data= BytesIO(metadata_check["data"].encode())
            url= metadata_check["metadata"]["url"]
            prefix=""
            url=url.split('/')
            n=len(url)
            for i in range(1,n-1):
                prefix=prefix+url[i]+'/'
            prefix=prefix+url[n-1]
            
          
            s3.put_object(Bucket='onepurchase-test-s3-datalakebucket',Key=prefix, Body=data, ServerSideEncryption='AES256',Metadata={
        'retention': retention,
        'usecase': usecase,
        'quality': quality,
        'refresh_velocity': refresh_velocity,
        'source': source,
        'ingestion_owner': ingestion_owner,
        'confidentiality': confidentiality,
        'expiration': expiration,
        'level1_ownership': level1_ownership,
        'level2_ownership': level2_ownership,
        'url': 'onepurchase-test-s3-datalakebucket/'+prefix
            })
            
            print("uploaded to datalake")

            
    return 'Operation is successfully Completed'
