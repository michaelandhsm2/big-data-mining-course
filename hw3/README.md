# Homework 3

## Description

### Data
[Reuters-21578 Text Categorization Collection Data Set](https://archive.ics.uci.edu/ml/datasets/reuters-21578+text+categorization+collection) - about 2 million instances, 28MB in size


News: 21 SGML files
We only deal with news contents inside &lt;body> ... &lt;/body> tags


### Task
4 subtasks:
+ (30pt) Given the Reuters-21578 dataset, please calculate all k-shingles and output the set representation of the text dataset as a matrix.
+ (30pt) Given the set representation, compute the minhash signatures of all documents using MapReduce.
+ (40pt) Implement the LSH algorithm by MapReduce and output the resulting candidate pairs of similar documents.

### Output Format

1. Set representation:
A MxN matrix: with rows as shingles and columns as documents (N=21,578)
2. minhash signatures:
The HxN signature matrix: with H as the number of hash functions, N=21,578
3. candidate pairs:
For each document i, there should be a list of those documents j>i with which i needs to be compared
4. comparison of KNN search an linear search


### Implementation Notes

Note the differences in rows and columns for the input data and the output matrices
* Input: rows as documents
* Output: columns as documents

The program should be able to accept some parameters:
* k in k-shingles
* Number of hash functions H


## Results

### Implementation Stack
Scala 2.11.8 + Spark 2.3.0

+ First Iteration (Local) - Jupyter Notebook + Apache Toree ([Code Demo](https://github.com/michaelandhsm2/big-data-mining-course/blob/master/hw3/HW%20%233.ipynb))
+ Second Iteration (Local) - Plain Scala ([Code](https://github.com/michaelandhsm2/big-data-mining-course/blob/master/hw3/sbt/src/main/scala/hw3.scala))
+ Third Iteration ([Google Dataproc](https://cloud.google.com/dataproc/))
  - 1 Master Node + 4 Worker Node
  - Machine Type: n1-highmem-4 (4vCPU, 26GB Memory, 50GB Disk)
![Cluster Setup Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw3/pics/Setup.png)

### Output
- Output File ([Google Drive Download Link](https://drive.google.com/file/d/1oIpQogkyDba7jlG7tm-cfuejQiTT-XPD/view?usp=sharing)), including:
  - Hash Function Matrix from Task 2 (For future hashing)
  - Signature Matrix from Task 2
  - LSH_Pairs from Task 3

- Shingle Matrix for Task 1 ([README.txt](https://github.com/michaelandhsm2/big-data-mining-course/blob/master/hw3/shingleMatrix.md))
  - Not Included in previous file due to its size.

- Console Output for Tasks ([Console Output Text](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw3/consoleLog.txt))

![Console Output 1 Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw3/pics/Results_4.png)
