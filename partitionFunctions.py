#!/usr/bin/env python
# File Name: partitionFunctions.py
#
# Date Created: Feb 17,2020
#
# Last Modified: Mon Feb 17 14:38:52 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
import pyspark
from functools import partial
from gourdian import gtypes


latLongPartitioner = lambda x: pyspark.sql.functions.round(x) #round to nearest line of latitude

epa_aqs_DatePartitioner = lambda x: x  #do nothing, already rounded to nearest day



