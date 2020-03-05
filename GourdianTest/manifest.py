#!/usr/bin/env python
# File Name: manifest.py
#
# Date Created: Mar 04,2020
#
# Last Modified: Wed Mar  4 16:17:29 2020
#
# Author: samolof
#
# Description:	
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


class Manifest(__RDict):
    def __init__(self, name, sources=[]):
        self.name = name
        self.sources = sources
        super().__init__(name = name, sources = sources)

class Source(__RDict):
    def __init__(self, name, layouts = [], tables=[]):
        self.name = name
        self.layouts = layouts
        self.tables  = tables
        super().__init__(name=name, layouts=layouts, tables=tables)

class Table(__RDict):
    def __init__(self,name, columns=[]):
        self.name = name
        self.columns = []
        super().__init__(name=name, columns = columns)

class Layout(__RDict):
    def __init__(self, name, label_columns=[], chunks=[]):
        self.name = name
        self.chunks = chunks
        self.label_columns = label_columns
        super().__init__(name=name, label_columns= label_columns, chunks=chunks)

class Chunk(__RDict):
    """ Chunk(filename= ,tag= , first_label= , last_label= , num_rows = ,)
    """
    pass
