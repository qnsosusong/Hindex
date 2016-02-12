from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import explode
from pyspark.sql.types import *
import re

# to get the collaboration information of the first author in each publication.

def aggToCassandra(agg):
    # save from Spark to Cassandra
     from cassandra.cluster import Cluster
     if agg:
         cascluster = Cluster(['52.35.154.160', '52.89.50.90', '52.89.58.183', '52.89.60.119'])
         casSession = cascluster.connect('test2')
         for aggItem in agg:
             if aggItem[0] != "":
                     casSession.execute('INSERT INTO author_collab_2 (author, collaborator, num_collab) VALUES (%s, %s, %s)', (aggItem[0], aggItem[1], aggItem[2]))
         casSession.shutdown()
         cascluster.shutdown()

sc = SparkContext("spark://ip-172-31-2-40:7077", "2016_test")
sqlContext = SQLContext(sc)

# read in raw data from HDFS, and select columns of 'recid', 'author', 'co_authors', and 'creation_date'
df = sqlContext.read.json("hdfs://ec2-52-35-154-160.us-west-2.compute.amazonaws.com:9000/camus/topics/raw_json_real/hourly/2016/01/20/16/raw_json_real*.gz")
df_author = df.dropna().select(df['recid'],df.authors[0].alias("author"), df['co-authors'].alias('co_authors'), df.creation_date)

# explode co_authors list, mapping author to each in the co_authors list
df_author_collaboration = df.dropna().select(df.authors[0].alias("author"), explode(df['co-authors'])).withColumnRenamed('_c0', 'collaborate')

# combine x.author and x.collaborate as the key, and count the number of each combined key. Then remap to x.author as the key, and {x.collaborate: x.num_collab} as the value.
rdd_collab = df_author_collaboration.dropna().rdd.map(lambda x: (x.author + '__' + x.collaborate, 1)).reduceByKey(lambda x, y: x + y).map(lambda x: {'author':x[0].split('__')[0], 'collab': x[0].split('__')[1], 'num_collab': x[1]})
rdd_collab.foreachPartition(agg)
