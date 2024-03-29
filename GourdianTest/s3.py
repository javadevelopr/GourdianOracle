#!/usr/bin/env python
# File Name: chunk.py
#
# Date Created: Feb 16,2020
#
# Last Modified: Tue Feb 25 18:12:03 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
import boto3
import botocore
from os.path import basename
import datetime
import hashlib
import os
import tempfile
import logging
from tagger import tag
from typing import Union, List, Dict, Optional, Callable

_sanitizeFN = lambda f: f.endswith('/') and f.rstrip('/') or f



def moveCanon(s3BucketName: str, s3canonPrefix: str, s3destPrefix: str):
    s3 = S3Operator(s3bucketName)
    
    #use filename extension(tag.timestamp) to get new timestamp of this version
    #and use as new folder name
    fileName = s3.getObjNames(s3canonPrefix)[0] 
    versionTimeStamp = fileName.split('.')[1]


    s3.moveAllFilesInFolder(s3canonPrefix, s3destPrefix + '/' + versionTimeStamp)




class S3Operator(object):
    
    
    def __init__(self, bucketName: str, aws_access_id: str=None, aws_secret_access_key: str=None):

        self.s3 = boto3.resource('s3')
        self.s3c = boto3.client('s3')
        self.bucket = self.s3.Bucket(bucketName)
        self.bucketName = self.bucket.name

    def getObjNames(self, prefix: str, ignoreFolders: bool = False):
        fileNames = []
        for obj in self.bucket.objects.filter(Prefix=prefix):
            key = obj.key
            if os.path.basename(key) == '_SUCCESS': #ignore Spark special file 
                continue
            if obj.key == prefix + "/": #ignore the base folder itself
                continue

            fileNames.append(key)

        if ignoreFolders:
            fileNames = list(filter(lambda f: not f.endswith('/'), fileNames))

        return fileNames

    def upload(self, filename:str, s3path:str, bucketName: Optional[str]=None):
        bucketName = bucketName or self.bucket.name
        try:
            selfs3c.upload_file(filename, bucketName, s3path)
        except OSError as e:
            raise

    def download(self, s3objName:str):
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

    def copyFile(self, s3srcPath:str, s3destPath:str):
        srcPath = f"{self.bucket.name}/{s3srcPath}"
        self.s3.Object(self.bucket.name, s3destPath).copy_from(CopySource=srcPath)

    def moveFile(self, s3srcPath:str, s3destPath:str):
        self.copyFile(s3srcPath, s3destPath)
        self.s3.Object(self.bucket.name, s3srcPath).delete()


    def createFolder(self,folderName:str, s3Prefix:str = None ):
        if prefix == "": 
            prefix = None

        key = prefix  and f"{prefix}/{folderName}/" or f"{folderName}/"
        resp = self.s3c.put_object(Bucket=self.bucketName, Key=key)

        if  resp['ResponseMetadata']['HTTPStatusCode'] not in range(200,210):
            raise 

    def moveAllFilesInFolder(self, s3srcFolder:str, s3destFolder: str):
        s3srcFolder = _sanitizeFN(s3srcFolder)
        s3destFolder = _sanitizeFN(s3destFolder)

        try:
            self.s3.Object(self.bucketName, s3destFolder + "/").load()

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                prefix = os.path.dirname(s3destFolder)
                folderName = os.path.basename(s3desFolder)

                self.createFolder(os.path.basename(s3destFolder), os.path.dirname(s3destFolder))
            else:
                raise

        fileNames = self.getObjNames(s3srcFolder, ignoreFolders=True)
        for f in fileNames:
            f = os.path.basename(f)
            self.moveFile(s3srcFolder + f"/{f}", s3destFolder + f"/{f}")
        
