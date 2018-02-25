# Hadoop 
############################################
## 1. Top 10 CommonWords in multiple files
### Hadoop + MapReduce 
1. Read mutiple files parallelly from HDFS, count the number of 'CommondWords' appering in these files at the same time excluding 'StopWords'. 
2. Sort these 'CommonWords' in descending order. 
3. Finally, pick up top 10 most frequent 'CommonWord' in the 'CommonWords list' generated in previous step.

## 2. Find Top-K most similar documents
### Hadoop + MapReduce + TF-IDF
#### Exact mathching + Ranked Retrieval Models + Bag of Word model + TF-IDF
1. Read multiple files from HDFS, use 'Bag of Word' model to compute the frequence of every word in every different file excluding 'StopWords'.
2. Compute -TF-IDF of every word w.r.t a document
3. Normalize TF-IDF of every word w.r.t a document
4. Compute the relevance of every document w.r.t query words --> Ranked Retrieval Models(mentioned above)
5. Sort documents according to the relevance to query words
