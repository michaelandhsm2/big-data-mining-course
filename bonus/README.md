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

+ First Iteration (Local) - Jupyter Notebook + Apache Toree ([Code Demo](https://github.com/michaelandhsm2/big-data-mining-course/blob/master/bonus/Bonus.ipynb))
+ Second Iteration (Local) - Plain Scala ([Code](https://github.com/michaelandhsm2/big-data-mining-course/blob/master/bonus/sbt/src/main/scala/bonus.scala))
+ Third Iteration ([Google Dataproc](https://cloud.google.com/dataproc/))
  - 1 Master Node + 3 Worker Node
  - Machine Type: n1-standard-1 (1vCPU, 3.75GB Memory, 10GB Disk)
![Cluster Setup Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/bonus/pics/Setup.png)

### Output
- Output File ([Google Drive Download Link](https://drive.google.com/file/d/1F9iSd-uqn65NOpeMyr1-kb3CGziukIxz/view?usp=sharing))

- Console Output for Task ([Console Output Text](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/bonus/consoleLog.txt))

![Console Output 1 Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/bonus/pics/Results_1.PNG)

![Console Output 2 Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/bonus/pics/Results_2.PNG)
