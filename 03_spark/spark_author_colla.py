from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import explode
from pyspark.sql.types import *
import re

def append_author(x, y):
    author = x        
    co_authors = y            
    co_authors.append(author)
    return co_authors

def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z

def H_index(item):
    x = item.values()
    cited_num = x.values()
    h_max = len(x) + 1
    for i in range(1, h_max):
        h  = len([t for t in cited_num if t >= i])
        if h < i:
            h_index = i - 1
            break
        else:
            h_index = i
    return h_index



def aggToCassandra(agg):
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

df = sqlContext.read.json("hdfs://ec2-52-35-154-160.us-west-2.compute.amazonaws.com:9000/camus/topics/raw_json_real/hourly/2016/01/20/16/raw_json_real*.gz")
df_author = df.dropna().select(df['recid'],df.authors[0].alias("author"), df['co-authors'].alias('co_authors'), df.creation_date)
df_author_collaboration = df.dropna().select(df.authors[0].alias("author"), explode(df['co-authors'])).withColumnRenamed('_c0', 'collaborate')
df_author_collaboration.rdd.foreachPartition(aggToCassandra)
