#!/usr/bin/env python
# File Name: parseJson.py
#
# Date Created: Feb 26,2020
#
# Last Modified: Thu Feb 27 15:46:09 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
import json
import argparse
import sys
import logger
from pyspark.sql import SparkSession
from chunk import Chunker
import functions

parser = argparse.ArgumentParser()
parser.add_argument('jsonFile', metavar='CONFIG_FILE', type=str, help='Json Configuration File')
parser.add_argument('--s3Root', metavar='S3_ROOT', type=str, help="The S3 Bucket containing all data sources.")

# partition_function = getattr(functions, 'longlatpartitioner')




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



if __name__=="__main__":
    spark = SparkSession.builder.appName('Gourdnet_Versioner').getOrCreate()
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)


    args = parser.args()
    jsFile = args.jsonFile

    chunkers = []; tables = {}
    js = None
    with open(jsFile, 'r') as j:
        js = json.loads(j.read())


    dataset = json["dataset"]["name"]
    sources =  json["dataset"]["sources"]

    for source in sources:
        name = source['name']
        columns = source['column']
        try:
            transformColumns = source['transformColumns']
            transformFunction = getUserFunctionFromName(source['transformFunction'])
    
        except KeyError as e:
            transformColumns = transformFunction = None



        layouts = source['layouts']

        for layout in layouts:
            layoutName, keys, keyFunctionNames = parseLayout(layout)

            keyFunctions = {}
            #replace function names in keyFunctions dict with actual functions if they exist
            for k,v in keyFunctionNames:
                keyFunctions[k] = getUserFunctionFromName(v)



            #Where the meat is. Create chunker and partition
            chunker = Chunker(
                    dataset=dataset,
                    source = source,
                    tableName = layoutName,
                    path = SOURCE_PATHS[dataset][source],
                    columns = columns,
                    keyColumns = keys,
                    keyFunctions = keyFunctions,
                    transformColumns = transformColumns,
                    transformFunction = transformFunction,
            )

            chunker.partition()




    spark.stop()
