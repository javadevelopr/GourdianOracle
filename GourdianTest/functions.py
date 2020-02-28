#!/usr/bin/env python
# File Name: transform.py
#
# Date Created: Feb 26,2020
#
# Last Modified: Wed Feb 26 21:56:13 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import lit as spark_lit
from pyspark.sql import dataframe as Dataframe
from functools import partial
#from gourdian import gtypes

from typing import Dict, Union, List



#=======TRANSFORMATION FUNCTIONS===
def noaa_transform(df: Dataframe, columns: Dict[str, List] ) -> Dataframe:

    #FRSHTT
    def __f(r, idx):
        v = list(str(x))
        return (v[idx] =='0' and False) or True

    transformFRSHTT = spark.udf("transformFRSHTT", __f, BooleanType())

    for i,c in enumerate( columns['FRSHTT']):
        df = df.withColumn(c, transformFRSHTT(df['FRSHTT'], spark_lit(i)))

    return df





#===============PARTITION KEY FUNCTIONS===================
#partition into 360 lines of latitude
latPartitioner = lambda x: round(x,1) 

#round to nearest meridien
longPartitioner = lambda x: round(x)


#do nothing, already rounded to nearest day
epa_aqs_DatePartitioner = lambda x: x  

