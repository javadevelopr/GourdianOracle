#!/usr/bin/env python
# File Name: partitionFunctions.py
#
# Date Created: Feb 17,2020
#
# Last Modified: Mon Feb 17 20:01:43 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
import pyspark
from functools import partial
from pyspark.sql.functions import round
from gourdian import gtypes


latPartitioner = lambda x: round(x,scale=1) 

#round to nearest meridien
longPartitioner = lambda x: round(x)


#do nothing, already rounded to nearest day
epa_aqs_DatePartitioner = lambda x: x  



