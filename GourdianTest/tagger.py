#!/usr/bin/env python
# File Name: tagger.py
#
# Date Created: Feb 22,2020
#
# Last Modified: Mon Feb 24 11:36:07 2020
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

from typing import List, Optional

def tag(dataset: str,source : str, tableName: str,  keyColumns:List[str], keyValues:List[str], canonicalChunkTag: Optional[str] = None) -> str:
    """
    Creates a 'tag' for a chunk or diff chunk:
    Tag = hash(dataset.source.tableName.keyColumn1:keyValue1|[keyColumn2:keyValue2|... ].[canonicalChunkTag].timestamp
    """

    b = f"{dataset}.{source}.{tableName}" + "|".join(x + ":" + y for x,y in zip(keyColumns, keyValues))
    base = hashlib.md5(b.encode('utf-8')).hexdigest()
    ext=datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    
    base = (canonicalChunkTag and f"{base}.{canonicalChunkTag}") or base

    return f"{base}.{ext}"




