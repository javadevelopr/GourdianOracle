#!/usr/bin/env python
# File Name: chunk.py
#
# Date Created: Feb 16,2020
#
# Last Modified: Tue Feb 25 16:08:05 2020
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


def moveAndTagS3Chunks(dataset: str, source: str, tableName: str, keyColumns: List[str], s3bucketName: str, s3bucketPrefix: str, delete:bool = False):
    """ 
    Spark doesn't seem to allow us to control output folder structure and filenames so we have to manually rename (tag) and 
    move the output files produced by it. 
    Chunks are stored as flat files at the top level of the AWS bucket.
    """


    def _getKeyValuesFromDirName(dirname):
        res = []
        keys = dirname.split('/')[1:]
        

        for k in keys: 
            res.append(k.split('=')[1])
        return res


    s3 = S3Operator(s3bucketName)

    files = s3.getObjNames(s3bucketPrefix)

    for f in files:

        #Get the key value from Spark output folder name
        dirname = os.path.dirname(f).replace(s3bucketPrefix,'')

        keyValues = _getKeyValuesFromDirName(dirname)

        fileTag = tag(dataset = dataset, source = source, tableName = tableName, keyColumns = keyColumns, keyValues = keyValues)

        #move file to top-level of bucket with fileTag as new filename
        if delete:
            s3.moveFile(f,fileTag)
        else:
            s3.copyFile(f,fileTag)

        logging.info(f"Moved {f} to {s3bucketName}/{fileTag}")
        




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
        key = prefix and f"{prefix}/{folderName}/" or f"{folderName}/"
        resp = self.s3c.put_object(Bucket=self.bucketName, Key=key)

        if  resp['ResponseMetadata']['HTTPStatusCode'] not in range(200,210):
            raise 

    def moveAllFilesInFolder(self, s3srcFolder:str, s3destFolder: str):
        try:
            s3.Object(self.bucketName, s3destFolder + "/").load()

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.createFolder(os.path.basename(s3destFolder), os.path.dirname(s3destFolder))
            else:
                raise

        fileNames = self.getObjectNames(s3srcFolder)
        for f in fileNames:
            self.moveFile(f, s3destFolder)
        
