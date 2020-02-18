#!/usr/bin/env python
# File Name: chunker.py
#
# Date Created: Feb 17,2020
#
# Last Modified: Mon Feb 17 23:54:07 2020
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
from pyspark.sql.types import StructField, DoubleType, StructType, IntegerType, StringType, TimeStampType
from functools import partial
from typing import Union
from gourdian import gtypes
import logging
from s3 import moveAndTagS3Chunks
from config import *




class Chunker:

    TMP_COLUMN_NAME_PREFIX="_0e02_c39fb0d2a21963b"
    
    def __init__(self, 
            dataset: str,
            source : str,
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
        self.path = path
        self.columns = columns
        self.max_chunk_length = max_chunk_length
        self.sortAscending = sortAscending

    
        #Probably generalize this more
        df = spark.read.load(path)
            .format(inputformat)
            .option('header', hasHeader)
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

    def writeParquetPartitions(self):
        if not self.isPartitioned:
            self.partition()

        destinationPath = "s3a://" + AWS_CHUNK_STORE_BUCKET + "/" + AWS_TMP_CHUNK_STORE_PATH
        writer = self.df.write.partitionBy(*self.partitionKeyColumns)
        writer.parquet(destinationPath, mode="overwrite")

        moveAndTagS3Chunks(self.dataset, self.source, self.keyColumns, AWS_CHUNK_STORE_BUCKET, AWS_TMP_CHUNK_STORE_PATH)

        print("================Partitions succesfully uploaded to S3=============")

    def writeCSVPartitions(self):
        pass
