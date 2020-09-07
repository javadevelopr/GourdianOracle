#!/usr/bin/env python
# File Name: manifest.py
#
# Date Created: Mar 04,2020
#
# Last Modified: Wed Mar  4 21:54:00 2020
#
# Author: samolof
#
# Description: JSON serializable Objects to represent Datasets, Sources, Layouts, Tables and Chunks	
#
##################################################################

class __RDict(dict):
    def __setitem__(self, key, value):
        validitems = self.__dict__.keys()
        if key in validitems:
            super().__setitem__(key,value)
    
    def __init__(self, **kwargs):
        for k,v in kwargs.items():
            self.__dict__[k] = v
            self[k] = v


class Dataset(__RDict):
    def __init__(self, name, sources=[]):
        super().__init__(name = name, sources = sources)

class Source(__RDict):
    def __init__(self, name, layouts = [], tables=[]):
        super().__init__(name=name, layouts=layouts, tables=tables)

class Layout(__RDict):
    def __init__(self, name, label_columns=[], chunks=[]):
        super().__init__(name=name, label_columns= label_columns, chunks=chunks)

class Table(__RDict):
    def __init__(self,name, columns=[]):
        super().__init__(name=name, columns = columns)

class Chunk(__RDict):
"""
I could write out the stub as for the classes above but this should work:
initialize with Chunk(filename= ,tag= , first_label= , last_label= , num_rows = )
"""
    pass
