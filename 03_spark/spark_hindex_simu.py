from pyspark.sql import SQLContext
from pyspark import SparkContext, StorageLevel
from pyspark.sql import Row
from pyspark.sql.functions import explode
from pyspark.sql.types import *
import re
import pandas as pd

def fetch_year(x):
        return int(re.search(r'\d{4}', x).group())

def append_author(x, y):
    co_authors = y
    co_authors.append(x)
    return co_authors

def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z

def H_index(item):
    cited_num = item.values()
    h_max = len(item) + 1
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
                    casSession.execute('INSERT INTO hindex_t01_simu (author, hindex) VALUES (%s, %s)', (aggItem[0], aggItem[1]))
        casSession.shutdown()
        cascluster.shutdown()


def aggToCassandra2(agg):
     from cassandra.cluster import Cluster
     if agg:
         cascluster = Cluster(['52.35.154.160', '52.89.50.90', '52.89.58.183', '52.89.60.119'])
         casSession = cascluster.connect('test2')
         for aggItem in agg:
             if aggItem[0] != "":
                     casSession.execute('INSERT INTO explode_author_pub_t01_simu (author, cited_id, num_cited, creation_date) VALUES (%s, %s, %s, %s)', (aggItem[2], aggItem[0], aggItem[1], aggItem[3]))
         casSession.shutdown()
         cascluster.shutdown()

def aggToCassandra3(agg):
    from cassandra.cluster import Cluster
    if agg:
        cascluster = Cluster(['52.35.154.160', '52.89.50.90', '52.89.58.183', '52.89.60.119'])
        casSession = cascluster.connect('test2')
        for aggItem in agg:
            if aggItem[0] != "":
                    casSession.execute('INSERT INTO author_pub_simu (author, pub) VALUES (%s, %s)', (str(aggItem[0]), str(aggItem[1])))
        casSession.shutdown()
        cascluster.shutdown()

sc = SparkContext("spark://ip-172-31-2-40:7077", "2016_test")
sqlContext = SQLContext(sc)

# read in data from HDFS and select columns
df1 = sqlContext.read.json("hdfs://ec2-52-34-128-244.us-west-2.compute.amazonaws.com:9000//simulated/fake_data_p1*.json").dropna()
df_sel = df1.select('recid', 'authors','co-authors','references', 'creation_date').withColumnRenamed('co-authors', 'co_authors').persist(StorageLevel.MEMORY_AND_DISK)

# explode references list and group by citation id to calcualte the number of times that one publication has been cited
df_references = df_sel.select('recid', explode('references')).withColumnRenamed('_c0','cited_id').groupBy('cited_id').count().withColumnRenamed('count','num_cited')

# combine author and co-author list to generate a total list of authors and convert rdd into dataframe
rdd_authors = df_sel.rdd.map(lambda x:{'recid':x.recid, 'authors': append_author(x.authors, x.co_authors), 'creation_date': fetch_year(x.creation_date)})
df_authors = sqlContext.createDataFrame(rdd_authors)

# join citation and author dataframes
df_join = df_references.join(df_authors, df_references.cited_id == df_authors.recid, 'inner').drop(df_authors.recid)

# explode author and save to Cassandra database
df_explode_author = df_join.select('cited_id', 'num_cited', explode('authors'), 'creation_date').withColumnRenamed('_c0', 'author')
df_explode_author.persist(StorageLevel.MEMORY_AND_DISK)
df_sel.unpersist()
df_explode_author.rdd.foreachPartition(aggToCassandra2)

# combine each author publication list,  group by author and calculate H-index for each author
test3 = df_explode_author.rdd.repartitioin(4096).map(lambda x:(x.author, {"cited_id": x.cited_id, "num_cited": x.num_cited}))
test4 = test3.reduceByKey(lambda x, y: merge_two_dicts(x, y)).map(lambda x: (x[0], H_index(x[1]) ))

# save both author publication list and H-index to Canssandra database
test4.foreachPartition(aggToCassandra)
test3.foreachPartition(aggToCassandra3)
