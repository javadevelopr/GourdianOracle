#!/usr/bin/env python
# File Name: parseJson.py
#
# Date Created: Feb 26,2020
#
# Last Modified: Thu Feb 27 07:37:31 2020
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

parser = argparse.ArgumentParser()
parser.add_argument('jsonFile', metavar='CONFIG_FILE',nargs='?', type=str, help='The Json Configuration File')
parser.add_argument('sourcePath')

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


if __name__=="__main__":
    spark = SparkSession.builder.appName('Gourdnet_Versioner').getOrCreate()
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
            transformFunctions = source['transformFunnction']
    
        except KeyError as e:
            transformColumns = transformFunctions = None

        layouts = source['layouts']

        for layout in layouts:
            layoutName, keys, keyFunctions = parseLayout(layout)

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
