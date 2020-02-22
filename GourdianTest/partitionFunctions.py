#!/usr/bin/env python
# File Name: partitionFunctions.py
#
# Date Created: Feb 17,2020
#
# Last Modified: Mon Feb 17 21:48:25 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
import pyspark
from functools import partial
from gourdian import gtypes

#partition into 360 lines of latitude
latPartitioner = lambda x: round(x,1) 

#round to nearest meridien
longPartitioner = lambda x: round(x)


#do nothing, already rounded to nearest day
epa_aqs_DatePartitioner = lambda x: x  



