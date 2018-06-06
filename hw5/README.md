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

+ First Iteration (Local) - Jupyter Notebook + Apache Toree ([Code Demo](https://github.com/michaelandhsm2/big-data-mining-course/blob/master/hw5/HW%20%235.ipynb))
+ Second Iteration (Local) - Plain Scala ([Code](https://github.com/michaelandhsm2/big-data-mining-course/blob/master/hw5/sbt/src/main/scala/hw5.scala))
+ Third Iteration ([Google Dataproc](https://cloud.google.com/dataproc/))
  - 1 Master Node + 3 Worker Node
  - Machine Type: n1-standard-1 (1vCPU, 3.75GB Memory, 10GB Disk)
![Cluster Setup Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw5/pics/Setup.PNG)

### Output
- Output File ([Google Drive Download Link](https://drive.google.com/file/d/1mUP5Ii8wMy2Vk0Ihkd1uK-tJjDm-IKXi/view?usp=sharing))

- Console Output for Task ([Console Output Text](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw5/consoleLog.txt))

![Console Output 1 Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw5/pics/Results_1.png)
