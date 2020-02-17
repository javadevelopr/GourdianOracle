#!/usr/bin/env python
# File Name: chunk.py
#
# Date Created: Feb 16,2020
#
# Last Modified: Mon Feb 17 02:39:35 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
import boto3
from os.path import basename
import datetime
import hashlib

def tag(dataset: str,source : str, key_column: str, key:str) -> str:
    """epa_aqs.ozone_daily_summary.latitude.18"""

    b = f"{dataset}.{source}.{key_column}.{key}"
    base = hashlib.md5(b.encode('utf-8')).hexdigest
    ext=datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    
    return f"{base}.{ext}"

#bucket = s3.Bucket('insight-gourdian-chunkstore')
#prefix='Test/epa_test_csv4/'

class S3Operator(object):
    
    def __init__(self, bucketName):
        access_key_id=os.environ['AWS_ACCESS_KEY_ID']
        secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

        self.s3 = boto3.resource('s3')
        self.s3c = boto3.client('s3')
        self.bucket = s3.Bucket(bucketName)
        self.prefix = prefix

    def getObjNamesS3(self, prefix):
        fileNames = []
        for obj in self.bucket.objects.filter(Prefix=prefix):
            fileNames.append(obj.key)

        return fileNames

    def uploadS3(self, filename, s3path):
        try:
            s3c.upload_file(filename, self.bucket.name, s3path)
            print(f"Uploaded {filename} to {bucket.name}/{s3path}")
        except OSError as e:
            pass

    def downloadS3(self, s3objName):
        try:
            s3c.download(self.bucket.name, s3objName, os.path.basename(s3objName))
        except OSError as e:
            pass
