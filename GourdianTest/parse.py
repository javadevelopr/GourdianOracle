#!/usr/bin/env python
# File Name: partition.py
#
# Date Created: Feb 04,2020
#
# Last Modified: Mon Feb 17 14:47:57 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
import boto3
import json
from pyspark.sql import SparkSession, functions as sqlFunctions
from pyspark.sql import dataframe
from gourdian import gytpes



def parseLayout(layout: dict, columns: list, gtype_columns: list):
    layout_name = layout['layout_name']
    layout_handler = LayoutHandlers[layout_name] 


def parseSource(sourceName: str, source: dict ):
        columns = source['columns'] 
        gtype_columns = source['gtype_columns']
        layouts = source['layouts']

        for layout in layouts:
            parseLayout(layout, columns, gtype_columns)
  
def getSourceFromRef(ref: str, sources: list) -> dict:
    """ A quirky way to handle repeated fields in json to keep the json file small. 
    For a key that repeats fields and values from another key we reference that key and
    parse it instead for the correct values
    """
    source = list(filter(lambda s: s['name'] == ref, sources))

    return len(source) > 0 and source[0] or None


def __s3getBucketSize(bucket):
    pass

def __getPartitionHash():
    pass



def noaa_transform(df: dataframe.Dataframe):
    pass



def epa_aqs_transform(df: dataframe.Dataframe ):

    #rename columns
    df = df.withColumnRenamed('Date Local','Date')


    #split columns


    #drop columns


    #change Type?



#To do transform column types to Gtypes
#df = spark.read.load("s3a://insight-gourdian-epaaqs-co/*.csv", format="csv", sep=",", inferSchema="true", header="true")
            


def setParameters(jsonFile: str):
    global datasetName 
    ref = None
    with open(jsonFile) as jfile:
        js = json.loads(jfile.read())
        datasetName = js['dataset']['name']
        sources = js['dataset']['sources']
        for source in sources:
            sourceName = source['name']
            ref = source['ref']
            if ref and len(ref) > 0: 
                source = getSourceFromRef(ref, sources)
            parseSource(sourceName, source)
            

        
def main():
   spark = SparkSession.builder.appName('Versioner').getOrCreate()


    



    spark.stop()

#load json
#

#spark=SparkSession.builder.appName('Versioner').getOrCreate()
#data=spark.read.load("../Data/chunk_noaa.csv",format="csv", sep=",", inferSchema="true", header="true" )

#print(data.columns)


