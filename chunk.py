#!/usr/bin/env python
# File Name: chunker.py
#
# Date Created: Feb 17,2020
#
# Last Modified: Mon Feb 17 14:39:03 2020
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
from functools import partial
from gourdian import gtypes

tempColumnName="_0e02_c39fb0d2a21963b"


#Load dataset from S3
#


