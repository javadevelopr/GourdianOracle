#!/usr/bin/env python
# File Name: chunker.py
#
# Date Created: Feb 17,2020
#
# Last Modified: Thu Feb 27 22:10:07 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import dataframe as Dataframe
from pyspark.sql.functions import round as spark_round, sha1 as spark_sha1
from pyspark.sql.functions import concat_ws as spark_concat_ws, lit as spark_lit
from pyspark.sql.types import StructField, DoubleType, StructType, IntegerType, StringType
from pyspark.accumulators import AccumulatorParam
from functools import partial
from enum import Enum
#from gourdian import gtypes
import logging
from s3 import moveAndTagS3Chunks
from tagger import tag
from typing import Union, List, Dict, Optional, Callable




PARTITION_COLUMN_NAME_PREFIX="_0e02_c39fb0d2a21963b_"
HASH_COLUMN_NAME="__GSHA1_KEY_"
DIFF_CODE_COLUMN_NAME="__GACTION_"
SAVE_MODE="overwrite"

class DiffCode(Enum):
    deletion = 0
    addition = 1



class _ListParam(AccumulatorParam):
    """ Creates a list accumulator for Spark 
        We need a list accumulator to gather the results of a call we make to forEachPartition()
    """
    def zero(self, v): return []
    def addInPlace(self, l1, l2):
        l1.extend(l2)
        return l1

#https://stackoverflow.com/questions/53010507/spark-dataframe-column-naming-conventions-restrictions
def cleanColumnNamesForParquet(df: Dataframe) -> Dataframe:
    newColumns = []
    nchars = ',;{}()='
    for c in df.columns:
        c = c.lower()
        c = c.replace(' ','_')
        for c in nchars:
            c = c.replace(c,'')
        newColumns.append(c)

    df = df.toDF(*newColumns)
    return df



class Loader(object):
    
    def read(self, spark: SparkSession, filePath: str, inputFormat: str ="csv", hasHeader: bool= True, inputDelimiter: str=",") -> Dataframe:
        df = spark.read.format(inputFormat)

        if inputFormat == "csv":
            df = df.option('header', hasHeader and "true" or "false") \
                .option('delimiter', inputDelimiter)           \
                .option('inferSchema', "true")
    
        df = df.load(filePath)
        return df

    def __init__(self, 
            spark: SparkSession,
            dataset: str,
            source : str,
            path: str,
            columns: Dict[str,str],
            transformColumns: List[str]  = [] ,
            transformFunction: callable = None,
            inputformat: str = "csv",
            hasHeader: bool = True,
            inputDelimiter: str = ","
        ):
    
        self.spark = spark
        self.dataset = dataset
        self.source = source
        self.path = path
        self.columns = columns

        logging.info("ABOUT TO READ DATASET") ###### 
        df = self.read(spark, path)

        logging.info(f"FINISHED READ DATASET: {df.count()}") ###### 

        #get rid of unneeded columns
        df = df.select(*columns.keys())
        

        logging.info(df.columns) ######

        #change column names and apply transformations
        for c in [col for col in df.columns if col not in transformColumns]:
            if type(columns[c]) == str :
                df = df.withColumnRenamed(c, columns[c])
        if transformFunction:        
            df = transformFunction(df, {k:columns[k] for k in transformColumns})

            
        self.df = df

        #add some internal columns for spark administration
        self.__adRowHash()


    def __addRowHash(self):
        """Add a column for hash of the row as a superkey"""
        self.df = self.df.withColumn(HASH_COLUMN_NAME, spark_sha1(spark_concat_ws("", *self.df.columns)))




class Partitioner(object):

    def __init__(
            self, 
            loader: Loader, 
            tableName: str,
            keyColumns: List[str],
            keyFunctions: Dict[str, callable] ={},
            sortAscending : bool = False

            ):
        self.sortAscending = sortAscending
        self.loader = loader
        
        #need to translate keyColumns to new column names
        self.keyColumns = list( map(lambda k: loader.columns[k], keyColumns))
        self.partitionKeyColumns = []

        #similarly for keyFunctions
        self.keyFunctions = {}
        for k,v in keyFunctions:
            self.keyFunctions[loader.columns[k]] = keyFunctions[k]

        #Get loaded dataframe and drop null for key columns
        self.df = loader.df.na.drop(subset=self.keyColumns)
        
        self.__createPartitionColumns()
        self.isPartitioned = False

    def __createPartitionColumns(self):

        for column in self.keyColumns:
            colName = PARTITION_COLUMN_NAME_PREFIX + column
            kf = self.keyFunctions.get(column)
            
            if kf:
                kf = self.spark.udf.register(column + "_udf", kf)
                self.df = self.df.withColumn(colName, kf(self.df[column]))
            else:
                self.df = self.df.withColumn(colName, self.df[column])

            self.partitionKeyColumns.append(colName)

    def getFirstAndLastChunkRows(self):
        first_and_last_rows_ac = sc.accumulator([], _ListParam())

        def __f(it):
            global first_and_last_rows_ac
            first = last = None
            try:
                first = next(it)
                *_, last = it

            except StopIteration: pass
            except ValueError:
                last = first
            first_and_last_rows_ac += [(first,last)]

        self.df.rdd.foreachPartition(__f)

        first_and_last_labels = list( map(lambda x: tuple([x[c] for c in self.keyColumns]), first_and_last_rows_ac.value))

        return first_and_last_labels



    def partition(self):
        self.df = self.df.repartition(*self.partitionKeyColumns).sortWithinPartitions(*self.keyColumns, ascending = self.sortAscending)
        self.isPartitioned = True

        #get first and last label
        #write manifest


    def diff(self): 
        """ We run a 2-way subtract to get rows added and rows deleted. Changed rows are just rows simultaenously added to the new version
            and deleted from the old version.
        """
        
        #fetch current canon
        canon_df = self.loader.read(self.loader.spark, AWS_CANON_STORE_PATH, inputFormat="parquet")

        additions = self.df.subtract(canon_df)
        deletions = canon_df.subtract(self.df)

        additons = additions.withColumn(DIFF_CODE_COLUMN_NAME, lit(DiffCode.addition))
        deletions = deletions.withColumn(DIFF_CODE_COLUMN_NAME, lit(DiffCode.deletion))

        #controversial
        self.df = additions.unionAll(deletions)
        self.partition()

    def writeParquetPartitions(self):
        if not self.isPartitioned:
            self.partition()

        destinationPath =  AWS_CHUNK_STORE_PATH + "/TMP"  
    
        writer = self.df.write.partitionBy(*self.partitionKeyColumns)
        writer.parquet(destinationPath, mode="overwrite")

        moveAndTagS3Chunks(self.dataset, self.source, self.tableName, self.keyColumns, AWS_CHUNK_STORE, "TMP")


    def writeCSVPartitions(self):
        pass


