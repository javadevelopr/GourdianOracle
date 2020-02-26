#!/usr/bin/env python
# File Name: chunker.py
#
# Date Created: Feb 17,2020
#
# Last Modified: Wed Feb 26 08:32:44 2020
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
from pyspark.sql.types import StructField, DoubleType, StructType, IntegerType, StringType, TimeStampType
from pyspark.accumulators import AccumulatorParam
from functools import partial
from enum import Enum
from gourdian import gtypes
import logging
from s3 import moveAndTagS3Chunks
from tagger import tag
from config import *
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



class Chunker:

    def _read(filePath: str, inputFormat: str ="csv", hasHeader: bool= True, inputDelimiter: str=",") -> Dataframe:
        df = spark.read.format(inputFormat)

        if inputFormat == "csv":
            df = df.option('header', hasHeader and "true" or "false").option('delimiter', inputDelimiter)
                    .option('inferSchema', "true")
    
        df = df.load(filePath)
        return df

    def __init__(self, 
            dataset: str,
            source : str,
            tableName: str,
            path: str,
            columns: Dict[str,str],
            keyColumns : List[str], 
            keyFunctions: Dict[str, callable] ={},
            sortAscending: bool = False, 
            transformColumns: List[str]  = [] ,
            transformFunction: Optional[ Callable[[Dataframe, Dict[str,str] ], Dataframe] ] = None,
            inputformat: str = "csv",
            hasHeader: bool = True,
            inputDelimiter: str = ","
        ):

        self.dataset = dataset
        self.source = source
        self.tableName = tableName
        self.path = path
        self.columns = columns
        self.max_chunk_length = max_chunk_length
        self.sortAscending = sortAscending

    
        df = _read(path)

        #get rid of unneeded columns
        df = df.select(*columns.keys())
            
        #drop null for key columns
        df = df.na.drop(subset=keyColumns)

        #change column names and apply transformations
        non_transform_columns = [c for c in df.columns if c not in transformColumns]
        for c in non_transform_columns:
            if type(columns[c]) == str :
                df = df.withColumnRenamed(c, columns[c])
        if transformFunction:        
            df = transformFunction(df, columns)

        #need to translate self.keyColumns to new column names
        self.keyColumns = list( map(lambda k: columns[k], keyColumns))
        self.partitionKeyColumns = []

        #similarly for keyFunctions
        self.keyFunctions = {}
        for k,v in keyFunctions:
            self.keyFunctions[columns[k]] = keyFunctions[k]

            
        self.df = df

        #add some internal columns for administration
        self.__adRowHash()
        self.__createPartitionColumns()

        self.isPartitioned = False


    def __createPartitionColumns(self):

        for column in self.keyColumns:
            colName = PARTITION_COLUMN_NAME_PREFIX + column
            kf = self.keyFunctions.get(column)
            
            if kf:
                kf = spark.udf.register(column + "_udf", kf)
                self.df = self.df.withColumn(colName, kf(self.df[column]))
            else:
                self.df = self.df.withColumn(colName, self.df[column])

            self.partitionKeyColumns.append(colName)


    def __addRowHash(self):
        """Add a column for hash of the row as a superkey"""
        self.df = self.df.withColumn(HASH_COLUMN_NAME, spark_sha1(spark_concat_ws("", *self.df.columns)))



    def __fixColumnNamesForParquet(self):
        newCols = []
        disallowed = ',;{}()='
        for column in self.df.columns:
            column = column.lower()
            column = column.replace(' ', '_')
            for c in disallowed:
                column = column.replace(c,'')
            
            newCols.append(column)
        self.df = self.df.toDF(*newCols)

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
        canon_df = self._read(S3_CANON_PATH, inputFormat="parquet")

        additions = self.df.subtract(canon_df)
        deletions = canon_df.subtract(self.df)

        additons = additions.withColumn(DIFF_CODE_COLUMN_NAME, lit(DiffCode.addition))
        deletions = deletions.withColumn(DIFF_CODE_COLUMN_NAME, lit(DiffCode.deletion))

        #controversial
        self.df = additions.unionAll(deletions)
        self.partition()

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


    def writeParquetPartitions(self):
        if not self.isPartitioned:
            self.partition()

        destinationPath = "s3a://" + AWS_CHUNK_STORE_BUCKET + "/" + AWS_TMP_CHUNK_STORE_PATH
        writer = self.df.write.partitionBy(*self.partitionKeyColumns)
        writer.parquet(destinationPath, mode="overwrite")

        moveAndTagS3Chunks(self.dataset, self.source, self.tableName, self.keyColumns, AWS_CHUNK_STORE_BUCKET, AWS_TMP_CHUNK_STORE_PATH)


    def writeCSVPartitions(self):
        pass
