#!/usr/bin/env python
# File Name: chunker.py
#
# Date Created: Feb 17,2020
#
# Last Modified: Mon Feb 17 21:46:22 2020
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
from pyspark.sql.types import StructField, DoubleType, StructType, IntegerType
from functools import partial
from typing import Union
from gourdian import gtypes
import logging

tempColumnName="_0e02_c39fb0d2a21963b"



class Chunker:
    def __init__(self, 
            dataset: str,
            source : str,
            path: str,
            columns: dict,
            keyColumns : list, 
            sortOrder: str = "desc", 
            keyFunction: callable = None,
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
        self.sortOrder = sortOrder
        self.keyFunction = keyFunction

    
        #obviously will generalize this
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
    

        #finally transform columns using passed callable
        if transform:
            self.sparkDF = transform(sparkDF)
        else
            self.sparkDF = sparkDF


    def partition(self):
        #sort
        #sortOrderStr = "desc" in self.sortOrder and "desc" or ""
        #_stk = []
        #for k in self.layoutKeys:
        #    _stk.append(k + " " + sortOrderStr)
        
        #sortedDF = self.sparkDF.orderBy(*_stk)

        #partition by key and checking max_chunks   
        self.sparkDF.rdd.partitionBy(  , self.partitioner) 


    
