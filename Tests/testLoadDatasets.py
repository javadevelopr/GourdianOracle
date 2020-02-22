from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext as sc
from pyspark.sql import functions as sqlFunctions
from pathlib import Path

spark = SparkSession.builder.appName("GourdianTestLoad").getOrCreate()



import time
#time.sleep(200)

epa_sources= list(map(lambda s: f"s3a://insight-gourdian-epaaqs-{s}/*.csv", [
    "ozone", "no2","so2","co"
]))


noaa_sources = [ "s3a://insight-gourdian-noaa-global-summary-of-day-joined/*.csv" ]

total_rows ={}
import time
start_time = time.time()
for source in epa_sources + noaa_sources:
    df = spark.read.format("csv").options(header="true", inferSchema="true", sep=",").load(source)

    count = df.count()
    total_rows[source] = count
    print(f"Finished {source}\nTime: {time.time() - start_time}")


print("Done")

for k,v in total_rows.items():
    print(f"{k} : {v} rows")

print(f"TOTAL TIME: {(time.time() - start_time)/60.} mins -----")

spark.stop()
