#!/usr/bin/env python
# File Name: chunk.py
#
# Date Created: Feb 16,2020
#
# Last Modified: Thu Feb 20 09:35:47 2020
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
import os
import tempfile
import logging


def tag(dataset: str,source : str, tableName: str,  keyColumns:list, keyValues:list, canonicalChunkTag: str = None) -> str:
    """
    Creates a 'tag' for a chunk or diff chunk:
    Tag = hash(dataset.source.tableName.keyColumn1:keyValue1|[keyColumn2:keyValue2|... ].[canonicalChunkTag].timestamp
    """

    b = f"{dataset}.{source}.{tableName}" + "|".join(x + ":" + y for x,y in zip(keyColumns, keyValues))
    base = hashlib.md5(b.encode('utf-8')).hexdigest()
    ext=datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    
    base = (canonicalChunkTag and f"{base}.{canonicalChunkTag}") or base

    return f"{base}.{ext}"




def moveAndTagS3Chunks(dataset: str, source: str, tableName: str, keyColumns: list, s3bucketName: str, s3bucketPrefix: str):
    """ 
    Spark doesn't seem to allow us to control output folder structure and filenames so we have to manually rename (tag) and 
    move the output files produced by it. 
    Chunks are stored as flat files at the top level of the AWS bucket.
    """


    def _getKeyValuesFromDirName(dirname):
        res = []
        keys = dirname.split('/')
        for k in keys: 
            res.append(k.split('=')[1])
        return res


    s3 = S3Operator(s3bucketName)

    files = s3.getObjNames(s3bucketPrefix)

    for f in files:
        #Get the key value from Spark output folder name
        keyValues = _getKeyValuesFromDirName(os.bath.dirname(f))

        fileTag = tag(dataset, source, keyColumns, keyValues)

        #move file to top-level of bucket with fileTag as new filename
        s3.move(f,fileTag)
        logging.info(f"Moved {f} to {s3bucketName}/{fileTag}")
        




class S3Operator(object):
    
    def __init__(self, bucketName):
        access_key_id=os.environ['AWS_ACCESS_KEY_ID']
        secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

        self.s3 = boto3.resource('s3')
        self.s3c = boto3.client('s3')
        self.bucket = s3.Bucket(bucketName)

    def getObjNames(self, prefix):
        fileNames = []
        for obj in self.bucket.objects.filter(Prefix=prefix):
            fileNames.append(obj.key)

        return fileNames

    def upload(self, filename, s3path, bucketName=None):
        bucketName = bucketName or self.bucket.name
        try:
            selfs3c.upload_file(filename, bucketName, s3path)
            print(f"Uploaded {filename} to {bucket.name}/{s3path}")
        except OSError as e:
            raise

    def download(self, s3objName):
        currentdir = os.getcwd()
        try:
            tempdir = tempfile.gettempdir()
            os.chdir(tempdir)
            self.s3c.download(self.bucket.name, s3objName, os.path.basename(s3objName))
        except OSError as e:
            raise
        finally:
            os.chdir(currentdir)
        
        return tempdir + "/" + os.path.basename(s3objName)

    def moveFile(self, s3srcPath, s3destPath):
        #self.s3.Object(self.bucket.name, s3destPath).copy_from(CopySource=s3destPath)
        #self.s3.Object(self.bucket.name, s3srcPath).delete()

        #s3 access configuration not working for me so temporary fix: download then upload file to move
        localFilePath = self.download(s3srcPath)
        self.upload(localFilePath, s3destPath)
