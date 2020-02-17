#!/usr/bin/env python
# File Name: chunker.py
#
# Date Created: Feb 17,2020
#
# Last Modified: Mon Feb 17 08:11:27 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import round
from pyspark.sql import dataframe as Dataframe
from gourdian import gtypes



def LatLongPartitioner(df : Dataframe) -> Dataframe
LatLongPartitioner = lambda x: x + round(x['Latitude'])

#Load dataset from S3
#


