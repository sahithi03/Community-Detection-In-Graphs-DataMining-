import os

from graphframes import GraphFrame
from pyspark import SparkContext
import sys
import time
from pyspark.sql import SQLContext
from collections import defaultdict
import csv

from pyspark.sql.types import StringType, StructType, StructField

sc = SparkContext('local[*]','task1')
sc.setLogLevel("ERROR")
os.environ["PYSPARK_SUBMIT_ARGS"] = ( "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")
start = time.time()
sqlContext = SQLContext(sc)

input_file_name = sys.argv[1]
output_file_name = sys.argv[2]

data = sc.textFile(input_file_name)
#construct graph


vertices_rdd1 = data.map(lambda x:x.split(' ')).map(lambda x:(str(x[0]),[str(x[1])])).reduceByKey(lambda x,y:x+y).map(lambda x:(x[0])).collect()
vertices_rdd2 = data.map(lambda x:x.split(' ')).map(lambda x:(str(x[1]),[str(x[0])])).reduceByKey(lambda x,y:x+y).map(lambda x:(x[0])).collect()
vertices_rdd = sc.parallelize(list(set(vertices_rdd1+vertices_rdd2))).map(lambda x:(x,))

edges1_rdd = data.map(lambda x:x.split(' ')).map(lambda x:(str(x[0]),str(x[1]))).collect()
edges2_rdd = data.map(lambda x:x.split(' ')).map(lambda x:(str(x[1]),str(x[0]))).collect()
edges1 = edges1_rdd + edges2_rdd
edges_rdd = sc.parallelize(edges1)


vertices = sqlContext.createDataFrame(vertices_rdd,["id"])


edges = sqlContext.createDataFrame(edges_rdd,["src","dst"])

#add edges and vertices to graph in library
g = GraphFrame(vertices,edges)
#print(g)

#call algorithm to detect communities
result = g.labelPropagation(maxIter=5).rdd.map(lambda x:(x[1],str(x[0]))).groupByKey().map(lambda x:sorted(x[1]))
sorted_graph_list = result.collect()

sorted_graph_list.sort()
sorted_graph_list.sort(key=len)

#print(sorted_graph_list)

f = open(output_file_name, 'w')

for i in sorted_graph_list:
	s= str(i).replace("[","").replace("]","")
	f.write(str(s))
	f.write("\n")
f.close()

print(time.time() - start)
