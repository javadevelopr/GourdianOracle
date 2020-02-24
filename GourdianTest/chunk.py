#!/usr/bin/env python
# File Name: chunker.py
#
# Date Created: Feb 17,2020
#
# Last Modified: Mon Feb 24 15:50:31 2020
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

class DiffCode(Enum):
    deletion = 0
    addition = 1



class _ListParam(AccumulatorParam):
    """Creates a list accumulator for Spark """
    def zero(self, v): return []
    def addInPlace(self, l1, l2):
        l1.extend(l2)
        return l1

PARTITION_COLUMN_NAME_PREFIX="_0e02_c39fb0d2a21963b_"
HASH_COLUMN_NAME="__GSHA1_KEY_"
DIFF_CODE_COLUMN_NAME="__GACTION_"



SAVE_MODE="overwrite"

from typing import Union, List, Dict, Optional, Callable

class Chunker:

    
    def __init__(self, 
            dataset: str,
            source : str,
            tableName: str,
            path: str,
            columns: Dict[str,str],
            keyColumns : List[str], 
            keyFunctions: Optional[ Dict[str, callable] ]={},
            sortAscending: Optional[bool] = False, 
            transform: Optional[ Callable[[Dataframe], Dataframe] ] = None,
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

        #need to translate keyColumns to new column names
        self.keyColumns = list( map(lambda k: columns[k], keyColumns))
        self.partitionKeyColumns = []

        #similarly for keyFunctions
        self.keyFunctions = {}
        for k,v in keyFunctions:
            self.keyFunctions[columns[k]] = keyFunctions[k]

        #finally transform columns using passed callable if any
        if transform:
            df = transform(df)
            
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


    def __fetchCanonDF(self):
        pass


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
        canon_df = self.__fetchCanonDF()

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
