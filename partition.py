#!/usr/bin/env python
# File Name: partition.py
#
# Date Created: Feb 04,2020
#
# Last Modified: Thu Feb  6 01:16:38 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
import boto3
import json
from pyspark.sql import SparkSession
from gourdian import gytpes


def approxlat_approxlng_yyyy_mm:
    pass

def yyyy_mm_approxlat_approxlng( fn: callable, *args):
    #fn is a method  that takes in *args (input column names, output column names) and
    #
    
    return partial(fn)
    pass

LayoutHandlers = {

    'approxlat_approxlng_yyyy_mm' : approxlat_approxlng_yyyy_mm,
    'yyyy_mm_approxlat_approxlng' : yyyy_mm_approxlat_approxlng


}


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


#To do transform column types to Gtypes
class Chunker:
    def __init__(self, path, layout_name, layoutKeys, columns, sortOrder = "desc", transform: callable = None ):
        self.path = path
        self.layout_name = layout_name
        self.layoutKeys = layoutKeys
        self.columns = columns
        self.max_chunk_length = max_chunk_length
        self.sortOrder = sortOrder

        #obviously will generalize this
        sparkDF = spark.read.load(path)
            .format('csv')
            .option('header','true')
            .option('delimiter', ",")
            .option('inferSchema', "true")

        #get rid of unneeded columns
        sparkDF = sparkDF.select(columns)

        #finally transform columns using passed callable
        self.sparkDF = transform(sparkDF)

    def partition(self):
        #sort
        sortOrderStr = "desc" in self.sortOrder and "desc" or ""
        _stk = []
        for k in self.layoutKeys:
            _stk.append(k + " " + sortOrderStr)
        
        sortedDF = self.sparkDF.orderBy(*_stk)

        

        #partition by key and checking max_chunks   
        key = divideByLatitude


    
            


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


