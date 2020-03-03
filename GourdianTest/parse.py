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
#import chunk
from chunk import Loader as Loader
from chunk import Partitioner
import functions
import configparser
from typing import Dict, Union, List

parser = argparse.ArgumentParser()
parser.add_argument('dataset', metavar='DATASET_NAME', type=str, help='Name of the Dataset to process e.g epa_aqs (see config.ini)')
parser.add_argument('--s3Root', metavar='S3_ROOT', type=str, help="The S3 Bucket containing all data sources.")


LOG_FORMAT = '[%(asctime)s %(filename)s:%(lineno)s] %(message)s'


def getUserFunctionFromName(functionName):
    try:
        return getattr(functions, functionName)
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
    #except KeyError:
    pass


if __name__=="__main__":
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)

    config = configparser.ConfigParser()
    config.read('config.ini')

    args = parser.parse_args()
    datasetName = args.dataset


    #set some global configuration values
    aws_chunk_store = config['aws']['ChunkStore']
    aws_chunk_store_path = f"s3a://{aws_chunk_store}"
    aws_diff_store = config['aws']['DiffStore']
    aws_diff_store_path = f"s3a://{aws_diff_store}"
    aws_canon_store_prefix = config['aws']['CanonStorePrefix']
    aws_canon_store_path= f"{aws_chunk_store_path}/{aws_canon_store_prefix}"


    logging.info("Loaded Configs") 

    #check that we have the json manifest for that dataset
    #and a config entry (with the source paths for that dataset)
    assert datasetName in config['json'].keys() and datasetName in config.sections()

    jsFile =  config['json'][datasetName]

    

    chunkers = []; tables = {}
    with open(jsFile, 'r') as j:
        js = json.loads(j.read())


    #assert datasetName = js["dataset"]["name"]
    sources =  js["dataset"]["sources"]

    spark = SparkSession.builder.appName('Gourdnet_Versioner') \
        .master(config['spark']['sparkMaster']) \
        .config('spark.executor.memory', config['spark']['executorMemory']) \
        .config('spark.executor.cores', config['spark']['executorCores']) \
        .config('spark.driver.memory',config['spark']['driverMemory']) \
        .config('spark.driver.cores',config['spark']['driverCores']) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("OFF") #controversial

    for source in sources:
        sourceName = source['name']

        #check that a source path exists in config file for this source
        assert sourceName in config[datasetName]


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
            transformColumns = []
            transformFunction = None

        #load the dataset
        dfloader = Loader(
                spark=spark,
                dataset=datasetName,
                source = sourceName,
                path = config[datasetName][sourceName],
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
                        keyFunctions = keyFunctions,
                        chunkStorePath = aws_chunk_store_path,
                        diffStorePath = aws_diff_store_path,
                        canonStorePath = aws_canon_store_path
            )

            #chunker.partition()
            chunker.writeParquetPartitions()

            chunkLabels = chunker.getFirstAndLastRowLabels()

            moveAndTagChunkFiles(dataset, ..., chunkLabels)################
                    #for f in fileNames:
                        part= int(f.split('-')[1]) 
                        first_and_last = chunkLabels[part]
                        tag = getTag(first_and_last)
            break ########



    spark.stop()
