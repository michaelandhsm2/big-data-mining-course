# Bonus

## Description

### Data
[Chicago Crime dataset](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2) - about 6 million reported records, 1.45G in size

### Task
3 subtasks:
+ (30pt) For the attributes ‘Primary type’ and ‘Location description’, output the list of each value and the corresponding frequency count, sorted in descending order of the count, respectively.
+ (30pt) Output the most frequently occurred ‘Primary type‘ for each possible value of ‘Location description’, sorted in descending order of the frequency count.
+ (40pt) Output the most frequently occurred street name in the attribute ‘Block‘ for each ‘Primary type’, sorted in descending order of the frequency count. (You should remove the numbers in the ‘Block’ address of a street/avenue/boulevard)
+ (Bonus) From the attribute ‘Date’, extract the time in hours and output the most frequently occurred hour for each ‘Primary type’ and ‘Location description’, sorted in descending order of the frequency count, respectively.

### Output Format
1. two sorted lists of (value, count)
    + Sorted list of ‘Primary type’
    + Sorted list of ‘Location description’
1. n sorted lists of ‘Primary type’ for each ‘Location description’
    + n: # of possible values for ‘Location description’
1. n sorted list of street names for each ‘Primary type’
    + n: # of possible values for ‘Primary type’
1. two types of sorted lists:
    + Sorted lists of hours for each possible ‘Primary type’
    + Sorted lists of hours for each possible ‘Location description’

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
