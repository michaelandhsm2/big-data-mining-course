# Homework 4

## Description

### Data
[Reuters-21578 Text Categorization Collection Data Set](https://archive.ics.uci.edu/ml/datasets/reuters-21578+text+categorization+collection) - about 2 million instances, 28MB in size


News: 21 SGML files
We only deal with news contents inside <body> </body> tags
The other files will not be needed in this homework

### Format
One text file consisting of lines of records.

Each record contains 9 attributes separated by semicolons:


Reuters-21578, Distribution 1.0 includes five files (all-exchanges-strings.lc.txt, all-orgs-strings.lc.txt, all-people-strings.lc.txt, all-places-strings.lc.txt, and all-topics-strings.lc.txt) which list the names of *all* legal categories in each set. A sixth file, cat-descriptions_120396.txt gives some additional information on the category sets.


### Task
4 subtasks:
+ (30pt) Given the Reuters-21578 dataset, please calculate the term frequencies, and output the representation of the document contents as a term-document count matrix.
+ (30pt) Implement matrix multiplication by MapReduce. Your program should be able to output the result in appropriate dimensions.
+ (40pt) Given the term-document matrix in (1), compute the SVD decomposition of the matrix using MapReduce. Output the resulting eigenvalues and eigenvectors.

## Output Format

1. Term-Document Matrix:
A MxN matrix: with rows as term frequencies and columns as documents (N=21,578)
2. Result of Matrix Multiplication:
A MxR matrix: (MxN) * (NxR)
3. Eigen Pairs:
Eigenvalues sorted in descending order, and their corresponding eigenvectors
4. Eigen Pairs:
Eigenvalues sorted in descending order, and their corresponding eigenvectors


## Results

### Implementation Stack
Scala 2.11.8 + Spark 2.3.0

+ First Iteration (Local) - Jupyter Notebook + Apache Toree ([Code Demo](https://github.com/michaelandhsm2/big-data-mining-course/blob/master/hw4/HW%20%234.ipynb))
+ Second Iteration (Local) - Plain Scala ([Code](https://github.com/michaelandhsm2/big-data-mining-course/blob/master/hw4/sbt/src/main/scala/hw4.scala))
+ Third Iteration ([Google Dataproc](https://cloud.google.com/dataproc/))
  - 1 Master Node + 4 Worker Node
  - Machine Type: n1-highmem-4 (4vCPU, 26GB Memory, 50GB Disk)
![Cluster Setup Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw4/pics/Setup.png)

### Output
- Output File RAR ([Google Drive Download Link](https://drive.google.com/open?id=1sG1riiXrLLcf4StE0s_RLIvhd7T1i0Up)), including:
  - Partial Term-Document Count Matrix from Task 1
    -  Some of the results are ommited due to the size of the files.
  - Matrix Multiplication Result from Task 2
  - SVD Eigen Pairs from Task 3
    - Only used 100 news articles as source.

- Console Output for Tasks ([Console Output Text](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw4/consoleLog.txt))
![Console Output 1 Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw4/pics/Results_1.png)
