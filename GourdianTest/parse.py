#!/usr/bin/env python
# File Name: parseJson.py
#
# Date Created: Feb 26,2020
#
# Last Modified: Thu Feb 27 21:47:31 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
import json
import argparse
import sys
import logging
from pyspark.sql import SparkSession
from chunk import Loader, Partitioner
import functions
from typing import Dict, Union, List

parser = argparse.ArgumentParser()
parser.add_argument('jsonFile', metavar='CONFIG_FILE', type=str, help='Json Configuration File')
parser.add_argument('--s3Root', metavar='S3_ROOT', type=str, help="The S3 Bucket containing all data sources.")




SOURCE_PATHS = {
        'epa_aqs' : {
            'ozone_daily_summary' : "s3a://insight-gourdian-epaaqs-ozone-m/",
            'so2_daily_summary' : "s3a://insight-gourdian-epaaqs-so2/",
            'co_daily_summary' : "s3a://insight-gourdian-epaaqs-co/",
            'no2_daily_summary': "s3a://insight-gourdian-epaaqs-no2/"
            },
        'noaa': {
            "global_summary_of_day" : "s3a://insight-gourdian-noaa-global-summary-of-day-joined/"
        },
        'usgs_comcat' : {
            "summary": "s3a://insight-gourdian-sources/usgs_comcat/"
        }
}


LOG_FORMAT = '%[(asctime)s %(filename)s:%(lineno)s] %(message)s'


def getUserFunctionFromName(functionName):
    try:
        return getattr(function, functionName)
    except NameError:
        return None

def getJsonFieldFromRef(ref: str, jsonField: List[any]) -> any:
    """ A quirky way to handle repeated fields in json to keep the json file small. 
    For a key that repeats fields and values from another key we reference that key and
    parse it for the correct values.
    jsonField should be a list. The ref references another item in a list of fields
    """

    fields = list(filter(lambda s: s['name'] == ref, jsonField))
    return len(fields) > 0 and fields[0] or None



def recursiveParse(fieldName, jsonField: any):
    #fieldName = so2_daily_summary
    #try:
     #   if jsonField["ref"]     

    #except KeyError:
    #    continue
    pass

if __name__=="__main__":
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)


    args = parser.parse_args()
    jsFile = args.jsonFile

    chunkers = []; tables = {}
    js = None
    with open(jsFile, 'r') as j:
        js = json.loads(j.read())


    datasetName = js["dataset"]["name"]
    sources =  js["dataset"]["sources"]

    spark = SparkSession.builder.appName('Gourdnet_Versioner').getOrCreate()

    for source in sources:
        sourceName = source['name']

        try:
            ref = source['ref']
            if ref and len(ref) > 0:
                source = getJsonFieldFromRef(ref,sources)

        except KeyError:
            pass

        columns = source['columns']
        try:
            transformColumns = source['transformColumns']
            transformFunction = getUserFunctionFromName(source['transformFunction'])
    
        except KeyError as e:
            transformColumns = transformFunction = None

        #load the dataset
        dfloader = Loader(
                dataset=datasetName,
                source = sourceName,
                path = SOURCE_PATHS[dataset][source],
                columns = columns,
                transformColumns = transformColumns,
                transformFunction = transformFunction,
        )


        layouts = source['layouts']

        for layout in layouts:
            layoutName = layout['name']
            keys = layout['keys']
            keyFunctionNames = layout['keyFunctions']
    
            keyFunctions = {}
            #replace function names in keyFunctions dict with actual functions if they exist
            for k,v in keyFunctionNames.items():
                keyFunctions[k] = getUserFunctionFromName(v)

            

            chunker = Partitioner(
                        loader=dfloader, 
                        tableName = layoutName,
                        keyColumns = keys,
                        keyFunctions = keyFunctions
            )

            chunker.partition()
            




    #spark.stop()
