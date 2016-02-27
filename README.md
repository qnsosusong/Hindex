Research H-index

Research H-index is the project developed for Insight Data Engineering program through Jan. -- Feb. 2016.

## Motivation:

H-index is one of the metrics to evaluate the impact of a researcher in academia. It's suggested in 2005 by Jorge E. Hirsch, a physicist at UCSD. H-index is an author-level metric that attempts to measure both the productivity and citation impact of the publications of a scientist or scholar. For example, if a researcher's h-index is 6, he has 6 papers that have been cited at least 6 times. 

## Functions:
Research H-index provides the API to query on one resercher's publication and the citation information of each publication. 
![Alt Text](https://github.com/qnsosusong/Hindex/blob/master/04_images/API_demo.png "api")

The web UI enables the visualization of the author's publication and collaboration as the first author.
![Alt Text](https://github.com/qnsosusong/Hindex/blob/master/04_images/UI_query.png "service")

## Demo:



## Data pipeline
![Alt Text](https://github.com/qnsosusong/Hindex/blob/master/04_images/pipeline.png "Data Pipeline")

There's one batch processing for the project. The details of the batch processing pipeline is as follows:
- Kafka: inject raw data into two topics: real data (a set of high energy physics publications) and simulated data. The simulated data follows the schema of the real data is generated for scaling up.
- Camus: configed to consume messages from kafka and push them into hdfs
- Apache Spark: batch processing the publications records and calculate the H-index for each individual authors.
- Cassandra: save results from Spark and feed queries from the front end
- Flask: query Cassandra and send the results as HTML/JavaScript pages to the browser

## Raw data:
The real data is a set of high energ physics publications published from 1961 to 2015. Each record is a record of one publication, including information like author, co-authors, references and creation-date. 
The simulated data was generated following the schema of the real dataset, and used for scaling up.

## H-index calculation:
One publication contains a list of references (~ 30 on average) and a list author/co-authors (~ 50 on average). The first step is to break the link between the publication and the references. Secondly count how many times one publication has been cited and then group the publications by individual authors. After the (author, publication_list) is ready, I can H-index for individual author. 

## Challenges:
In this calculation process, I need to explode the record with first reference list and then the author list. So as the number of records increases, this quickly exceeded the memory and crashed the excutors in the cluster. Careful tuning in Spark is required to ensure a stable calculation. 
