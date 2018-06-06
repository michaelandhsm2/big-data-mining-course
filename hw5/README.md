# Homework 5

## Description

### Data
[Google web graph dataset](http://snap.stanford.edu/data/web-Google.html) - about 5 million edwges, 20MB (compressed) in size

Nodes represent web pages and directed edges represent hyperlinks between them.
The data was released in 2002 by Google as a part of Google Programming Contest.


### Task
3 subtasks:
+ (30pt) Given the Google web graph dataset, please output the list of web pages with the number of outlinks, sorted in descending order of the out-degrees.
+ (30pt) Please output the inlink distribution of the top linked web pages, sorted in descending order of the in-degrees.
+ (40pt) Design an algorithm that maintains the connectivity of two nodes in an efficient way. Given a node v, please output the list of nodes that v points to, and the list of nodes that points to v.
+ (Bonus) Compute the PageRank of the graph using MapReduce.

### Output Format
1. a sorted list of pages with their out-degrees
    + Each line contains: <NodeID\>, <out-degree\>
1. a sorted list of pages with their in-degrees
    + Each line contains: <NodeID\>, <in-degree\>
1. Given a node v,
    + The first line contains a list of nodes that v points to:
        + <ToNodeID\>, …, <ToNodeID\>
    + The second line contains a list of nodes point to v
        + <FromNodeID\>, …, <FromNodeID\>
1. The PageRank of each node in steady state
    + Each line contains: <NodeID\>, <Score\>

## Results

### Implementation Stack
Scala 2.11.8 + Spark 2.3.0

+ First Iteration (Local) - Jupyter Notebook + Apache Toree ([Code Demo](https://github.com/michaelandhsm2/big-data-mining-course/blob/master/hw1/HW%20%231.ipynb))
+ Second Iteration (Local) - Plain Scala ([Code](https://github.com/michaelandhsm2/big-data-mining-course/blob/master/hw1/sbt/src/main/scala/hw1.scala))
+ Third Iteration ([Google Dataproc](https://cloud.google.com/dataproc/))
  - 1 Master Node + 3 Worker Node
  - Machine Type: n1-standard-1 (1vCPU, 3.75GB Memory, 10GB Disk)
![Cluster Setup Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw1/pics/Cluster_Setup.PNG)

### Output
- Output File ([Google Drive Download Link](https://drive.google.com/file/d/1xTLz6hsYr96O0JV0eOBXtGXT3J5qT1mM/view?usp=sharing)), including:
  - 48 Segmented Files from Task 1 (Average Popularity)
  - 580 Files from Task 3 & 4 (Word Count & Co-occurance)

- Console Output for Task 1 ([Console Output Text](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw2/consoleLog_task1.txt))

![Console Output 1 Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw2/pics/Results_1.png)
