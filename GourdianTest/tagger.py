#!/usr/bin/env python
# File Name: tagger.py
#
# Date Created: Feb 22,2020
#
# Last Modified: Sat Feb 22 12:12:55 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
from os.path import basename
import datetime
import hashlib
import os
import tempfile

def tag(dataset: str,source : str, tableName: str,  keyColumns:list, keyValues:list, canonicalChunkTag: str = None) -> str:
    """
    Creates a 'tag' for a chunk or diff chunk:
    Tag = hash(dataset.source.tableName.keyColumn1:keyValue1|[keyColumn2:keyValue2|... ].[canonicalChunkTag].timestamp
    """

    b = f"{dataset}.{source}.{tableName}" + "|".join(x + ":" + y for x,y in zip(keyColumns, keyValues))
    base = hashlib.md5(b.encode('utf-8')).hexdigest()
    ext=datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    
    base = (canonicalChunkTag and f"{base}.{canonicalChunkTag}") or base

    return f"{base}.{ext}"




