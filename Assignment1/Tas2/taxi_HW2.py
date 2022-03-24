from __future__ import print_function
import sys
from operator import add
import os
import sys
import requests
from operator import add
import findspark

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *
from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Top10-Best Drivers <file> <output> ", file=sys.stderr)
        exit(-1)

findspark.init()
spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0.1 miles, 
# fare amount and total amount are more than 0.1 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0.10 and float(p[11])> 0.10 and float(p[16])> 0.10):
                return p

lines=sc.textFile(sys.argv[1])
# split content in the line separated by ','
taxi_lines=lines.map(lambda x:x.split(','))
#testDataFrame.show()
testRDD = taxi_lines.map(lambda x:(x[0], x[1], x[2], x[3], x[4], 
                    x[5], x[6], x[7], x[8], x[9], 
                    x[10],x[11],x[12], x[13], x[14], 
                    x[15], x[16]))
testRDD.take(1)
# calling isfloat and correctRows functions to cleaning up data
taxilinesCorrected = testRDD.filter(correctRows)
driver_triptime_totalamt=taxilinesCorrected.map(lambda p: (p[1],p[4],p[16]) )
# Agrregate trip time by driver and convert seconds to minutes
driver_agg_triptime = driver_triptime_totalamt.map(lambda n:  (str(n[0]), float(n[1]) ) ).aggregateByKey(0, lambda k,v: float(v)+k, lambda v,k: k+v).mapValues(lambda a:float(a/60))
# Agrregate total trip amount by driver 
driver_agg_totalamt = driver_triptime_totalamt.map(lambda n:  (str(n[0]), float(n[2]))).aggregateByKey(0, lambda k,v: float(v)+k, lambda v,k: k+v)

#Average amount earned by driver per minute
driver_average_earning= driver_agg_triptime.join(driver_agg_totalamt).mapValues(lambda x: x[1] / x[0])

#filter out top 10 drivers based on average amount earned per minut
driver_average_earning_top10= driver_average_earning.top(10, lambda x:x[1])
driver_average_earning_top10_top10result = spark.sparkContext.parallelize(driver_average_earning_top10)
driver_average_earning_top10_top10result.collect()
driver_average_earning_top10_top10result.coalesce(1).saveAsTextFile(sys.argv[2])
sc.stop()
spark.stop()
