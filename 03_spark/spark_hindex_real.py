from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import explode
from pyspark.sql.types import *
import re

def fetch_year(x):
	year = int(re.search(r'\d{4}', x).group())
	return year
def append_author(x, y):
    return co_authors = x + y

def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z

def H_index(item):
    # calculate H-index
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

def aggToCassandra1a(agg):
    # write the results of author h_hindex to table "hindex_t04"	
    from cassandra.cluster import Cluster
    if agg:
        cascluster = Cluster(['52.35.154.160', '52.89.50.90', '52.89.58.183', '52.89.60.119'])
        casSession = cascluster.connect('test2')
        for aggItem in agg:
            if aggItem[0] != "":
                    casSession.execute('INSERT INTO hindex_t04 (hindex, author) VALUES (%s, %s)', (aggItem[1], aggItem[0]))
        casSession.shutdown()
        cascluster.shutdown()


def aggToCassandra2(agg):
    # write the collaboration information of the first author of each publication to table 'explode_author3'
    from cassandra.cluster import Cluster
    if agg:
        cascluster = Cluster(['52.35.154.160', '52.89.50.90', '52.89.58.183', '52.89.60.119'])
        casSession = cascluster.connect('test2')
        for aggItem in agg:
            if aggItem[2] != "":
                    casSession.execute('INSERT INTO explode_author3 (author, num_cited, cited_num,  creation_date) VALUES (%s, %s, %s, %s)', (aggItem[2], aggItem[1], aggItem[0], aggItem[3]))
        casSession.shutdown()
        cascluster.shutdown()



def aggToCassandra3(agg):
    # write the (author, publication_list) to table 'author_pub'	
    from cassandra.cluster import Cluster
    if agg:
        cascluster = Cluster(['52.35.154.160', '52.89.50.90', '52.89.58.183', '52.89.60.119'])
        casSession = cascluster.connect('test2')
        for aggItem in agg:
            if aggItem[0] != "":
                    casSession.execute('INSERT INTO author_pub (author, pub) VALUES (%s, %s)', (aggItem[0], str(aggItem[1])))
        casSession.shutdown()
        cascluster.shutdown()


sc = SparkContext("spark://ip-172-31-2-40:7077", "2016_test")
sqlContext = SQLContext(sc)

# read in the simulated data and select the columns 'recid', 'authors', 'co-authors', and 'creation_date'
df = sqlContext.read.json("hdfs://ec2-52-35-154-160.us-west-2.compute.amazonaws.com:9000/camus/topics/raw_json_real/hourly/2016/01/20/16/raw_json_real*.gz").dropna().select('recid', 'authors','co-authors','references', 'creation_date').withColumnRenamed('co-authors', 'co_authors')

# explode the references list to make (recid, references) tuple. Group by 'cited_id' and count to calculate how many times one publication has been cited. 
df_references = df.select('recid', explode('references')).withColumnRenamed('_c0','cited_id').groupBy('cited_id').count().withColumnRenamed('count','num_cited')

# merge author and co_author to one list, and get the publication year as 'creation_date', and convert rdd to DataFrame 
rdd_authors = df.rdd.map(lambda x:{'recid':x.recid, 'authors': append_author(x.authors, x.co_authors), 'creation_date': fetch_year(x.creation_date)})
df_authors = sqlContext.createDataFrame(rdd_authors)

# join df_references and df_authors with cited_id == recid.
df_join = df_references.join(df_authors, df_references.cited_id == df_authors.recid, 'inner').drop(df_authors.recid).cache()

# explode merged author
df_explode_author = df_join.select('cited_id', 'num_cited', explode('authors'), 'creation_date').withColumnRenamed('_c0', 'author')

# map each record as (x.author, {x,cited_id: x.num_cited}) and group by author to make (author, publication list) with citaion information 
test3 = df_explode_author.rdd.map(lambda x:(x.author, {x.cited_id: x.num_cited})).reduceByKey(lambda x,y: merge_two_dicts(x, y))

# calculate H-index for each author 
test4 = test3.map(lambda x: (x[0], H_index(x[1])))

# write author collaboration info to Cassandra
df_explode_author.rdd.foreachPartition(aggToCassandra2)

 # write (author, publication list) to Cassandra
test3.foreachPartition(aggToCassandra3)

# write (author, hindex) to Cassandra
test4.foreachPartition(aggToCassandra)

# Create hindex_t04, save to DB using aggToCassandra1a
# CREATE TABLE hindex_t04  (hindex int, author text, PRIMARY KEY ((hindex),author), );
test4.foreachPartition(aggToCassandra1a)
