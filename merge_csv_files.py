#!/usr/bin/env python
# File Name: merge_csv_files.py
#
# Date Created: Feb 05,2020
#
# Last Modified: Wed Feb  5 02:57:11 2020
#
# Author: samolof
#
# Description:	
#
##################################################################
from os import listdir
from os.path import isfile,isdir, join
from pathlib import Path


path = Path('/data/noaa/global_summary_of_day/source/')

if __name__ == '__main__':
    dirs = [d for d in listdir(path) if isdir(d)]

    for d in dirs:
        dpath = Path('/data/noaa/global_summary_of_day/joined')/d
        dpath.mkdir(parents=True, exist_ok=True)
        files = [path/d/f for f in listdir(d)]
        if len(files) == 0: 
            continue
        print(f"len files: {len(files)}")###
        with open(dpath/(d + ".csv"), "w") as outfile:
            with open(files[0],'r') as first:
                outfile.write(first.read())

            for f in files[1:]:
                with open(f) as csvfile:
                    next(csvfile)
                    outfile.write(csvfile.read())





