from __future__ import print_function
import json
import base64
from io import BytesIO
import boto3
import ast
def lambda_handler(event, context):
    for record in event['Records']:
       #Kinesis data is base64 encoded so data is decoded here
       s3 = boto3.client('s3')
       payload=base64.b64decode(record["kinesis"]["data"])
       print("Decoded payload: " + str(payload))
      
       decoded_data = payload.decode("utf-8")
       datarecord = ast.literal_eval(decoded_data)
       
       print(datarecord)
       
       if not datarecord.get('metadata',{}).get('source',{}) or not datarecord.get('metadata',{}).get('retention',{})  or not datarecord.get('metadata',{}).get('business',{}) or not datarecord.get('metadata',{}).get('usecase',{}) or not datarecord.get('metadata',{}).get('ingestion_owner',{}) or not datarecord.get('metadata',{}).get('level1_ownership',{}) or not datarecord.get('metadata',{}).get('level2_ownership',{}) or not datarecord.get('metadata',{}).get('confidentiality',{})  or not datarecord.get('metadata',{}).get('quality',{})  or not datarecord.get('metadata',{}).get('refresh_velocity',{}):
           
           data=byte(payload.decode("utf-8"))
           url= datarecord["metadata"]["url"]
           prefix=""
           url=url.split('/')
           n=len(url)
           for i in range(1,n-1):
            prefix=prefix+url[i]+'/'
           prefix=prefix+url[n-1]
           s3.Bucket('<s3-backup-bucket>').put_object(Key=prefix, Body=data, ServerSideEncryption="AES256")
           print("Uploaded to Backup Bucket due to missing metadata tags")
    
           
           
       elif datarecord["metadata"]["source"]=='' or datarecord["metadata"]["retention"]=='' or datarecord["metadata"]["business"]==''  or datarecord["metadata"]["usecase"]==''  or datarecord["metadata"]["ingestion_owner"]==''  or datarecord["metadata"]["expiration"]==''  or datarecord["metadata"]["level1_ownership"]==''  or datarecord["metadata"]["level2_ownership"]==''  or datarecord["metadata"]["confidentiality"]==''  or datarecord["metadata"]["quality"]=='' or datarecord["metadata"]["refresh_velocity"]=='':
                       
            source=datarecord["metadata"]["source"]
            retention= datarecord["metadata"]["retention"]
            business=datarecord["metadata"]["business"]
            usecase=datarecord["metadata"]["usecase"]
            ingestion_owner=datarecord["metadata"]["ingestion_owner"]
            expiration=datarecord["metadata"]["expiration"]
            level1_ownership=datarecord["metadata"]["level1_ownership"]
            level2_ownership=datarecord["metadata"]["level2_ownership"]
            confidentiality=datarecord["metadata"]["confidentiality"]
            quality=datarecord["metadata"]["quality"]
            refresh_velocity=datarecord["metadata"]["refresh_velocity"] 
            data= BytesIO(datarecord["data"].encode())
            url= datarecord["metadata"]["url"]
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
           
            source=datarecord["metadata"]["source"]
            retention= datarecord["metadata"]["retention"]
            business=datarecord["metadata"]["business"]
            usecase=datarecord["metadata"]["usecase"]
            ingestion_owner=datarecord["metadata"]["ingestion_owner"]
            expiration=datarecord["metadata"]["expiration"]
            level1_ownership=datarecord["metadata"]["level1_ownership"]
            level2_ownership=datarecord["metadata"]["level2_ownership"]
            confidentiality=datarecord["metadata"]["confidentiality"]
            quality=datarecord["metadata"]["quality"]
            refresh_velocity=datarecord["metadata"]["refresh_velocity"]
            
            data= BytesIO(datarecord["data"].encode())
            url= datarecord["metadata"]["url"]
            prefix=""
            url=url.split('/')
            n=len(url)
            for i in range(1,n-1):
                prefix=prefix+url[i]+'/'
            prefix=prefix+url[n-1]
            
          
            s3.put_object(Bucket='<s3 target bucket>',Key=prefix, Body=data, ServerSideEncryption='AES256',Metadata={
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

