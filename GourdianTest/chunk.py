#!/usr/bin/env python
# File Name: chunker.py
#
# Date Created: Feb 17,2020
#
# Last Modified: Sat Feb 22 21:56:03 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
import boto3
from pyspark.sql import SparkSession
#from pyspark.sql.functions import round 
from pyspark.sql import dataframe as Dataframe
from pyspark.sql.functions import first,last
from pyspark.sql.types import StructField, DoubleType, StructType, IntegerType, StringType, TimeStampType
from pyspark.accumulators import AccumulatorParam
from functools import partial
from typing import Union
from gourdian import gtypes
import logging
from s3 import moveAndTagS3Chunks
from tagger import tag
from config import *

class _ListParam(AccumulatorParam):
    """Creates a list accumulator for Spark """
    def zero(self, v): return []
    def addInPlace(self, l1, l2):
        l1.extend(l2)
        return l1




class Chunker:

    TMP_COLUMN_NAME_PREFIX="_0e02_c39fb0d2a21963b"
    
    def __init__(self, 
            dataset: str,
            source : str,
            tableName: str,
            path: str,
            columns: dict,
            keyColumns : list, 
            sortAscending: bool = False, 
            transform: callable = None,
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

    
        #Probably generalize this more
        df = spark.read.load(path)
            .format(inputformat)
            .option('header', hasHeader and "true" or "false")
            .option('delimiter', inputDelimiter)
            .option('inferSchema', "true")

        #get rid of unneeded columns
        df = df.select(*columns.keys())
            
        #drop null for key columns
        df = df.na.drop(subset=keyColumns)

        #change column names including KeyColumns
        for c in df.columns:
            df = df.withColumnRenamed(columns[c])

        self.keyColumns = list( map(lambda k: columns[k], keyColumns))
        self.partitionKeyColumns = []

        #finally transform columns using passed callable if any
        if transform:
            df = transform(df)
            
        self.df = df

        self.isPartitioned = False

    def createPartitionColumn(self, columnName : str, 
            columnType: Union[IntegerType,DoubleType, StringType, TimeStampType],
            keyFunction: callable = None 
        ):

        newColumnName = TMP_COLUMN_NAME_PREFIX + columnName 
        
        schema = StructType(self.df.schema.fields + [StructField(newColumnName, columnType, True)])
        if keyFunction:
            self.df = self.df.rdd.map(lambda x: x + (keyFunction(x[columnName]),)).toDF(schema=schema)
        else
            self.df = self.df.rdd.map(lambda x: x + (x[columnName]),).toDF(schema=schema)

        self.partitionKeyColumns.append(newColumnName)

    def partition(self):
        self.df = self.df.repartition(*self.partitionKeyColumns).sortWithinPartitions(*self.keyColumns, ascending = self.sortAscending)
        self.isPartitioned = True

        #get first and last label

        #write manifest


    def _getPartitionTag(self):
        if not self.isPartitioned:
            self.partition()
        
        def __tag(it):
            pass


    def diff(self): 
        #Get partition Tag
        pass





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
