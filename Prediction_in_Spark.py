import sys 
from pyspark import SparkContext 
from numpy import array as np
from math import sqrt
from pyspark.mllib.clustering import KMeans, KMeansModel
import re

sc = SparkContext()

# Load and parse the data

data=sc.textFile("s3://budt758-sarthakp/additional/Q3_Final_2/part-v000-o000-r-00000.txt").map(lambda line: line.split("\t"))

sample = data.take(100000)
str1 = ' '.join(str(e) for e in sample)

qw=re.findall(r'\b\d+\b', str1)
c=[int(x) for x in qw]

lngth= len(c)


kmeans_data= array(c).reshape(lngth/2,2)
 
 model = KMeans.train(
...     sc.parallelize(kmeans_data), 100, maxIterations=10, initializationMode="random",
...                    seed=50, initializationSteps=5)

cluster_centers = model.clusterCenters

cluster_ind = model.predict(sc.parallelize(kmeans_data))
belonging_cluster = cluster_ind.collect()

print(cluster_centers,belonging_cluster);

