# Search with content-based filtering
A combination of a search engine and a content-based filtering recommendation system
MRJob implementation

## Usage
Search
```bash
python search.py <data_file> --search <search_terms> (−r hadoop) >  <output_file>
```
Recommend
```bash
python recommend.py <data_file> (−r hadoop) >  <output_file>
```

## Description
This project aims to make an search engine with content based filtering for the stack overflow questions data. A traditional search engine is a information retrieval system that allows the user to query a database to find information relevant to a keyword, ranked by their relevance. Formulating a search query is not always easy, as it is usually a few words, and sometimes the user may not even know exactly what he is looking for. Modern search engines use a vector space model as their base system, which is used in this project. In a vector space model, the document is represented as a numerical vector, and then similarity is computed between the query and the data. A search engine based on a vector space model allows to do content based filtering of the search results in order to provide a better coverage of the results. Content based filtering recommends items based on similarity between them. This is achieved by using different features of the posts to compute the TF-IDF score of them, and then calculate the similarity between the post and the search query. Information retrieval using the vector space model can be computationally expensive, as the similarity is computed between each post and the query. This is solved by distributing the data and computations to a computer cluster consisting of three slave nodes and a master node. The distribution is achieved using Apache Hadoop and Apache Spark, which are two frameworks that allows for distributed computations and storage of data. Both of these frameworks are based on MapReduce, which is a programming model for processing big datasets, and do computations in a distributed and parallel fashion on a cluster of computers. In the first approach we implement the TF-IDF algorithm and cosine similarity in pure python, and use the Map-Reduce (MR-JOB) of Hadoop to do the computations in a multi-processing fashion.
