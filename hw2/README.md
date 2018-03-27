# Homework 2

## Description

### Data
[News Popularity in Multiple Social Media Platforms Data Set](https://archive.ics.uci.edu/ml/datasets/News+Popularity+in+Multiple+Social+Media+Platforms) - 13 CSV files, 155MB in total  

This dataset contains a large set of news items and their respective social feedback on Facebook, Google + and LinkedIn.

### Task
4 subtasks:
+ (20pt) In social feedback data, calculate the average popularity of each news by hour, and by day, respectively
+ (20pt) In news data, calculate the sum and average sentiment score of each topic, respectively
+ (30pt) In news data, count the words in two fields: ‘Title’ and ‘Headline’ respectively, and list the most frequent words according to the term frequency in descending order, in total, per day, and per topic, respectively
+ (30pt) From the previous subtask, for the top-100 frequent words per topic in titles and headlines, calculate their co-occurrence matrices (100x100), respectively. Each entry in the matrix will contain the co-occurrence frequency in all news titles and headlines, respectively

## Results

### Implementation Stack
Scala 2.11.8 + Spark 2.3.0

+ First Iteration (Local) - Jupyter Notebook + Apache Toree ([Code Demo](https://github.com/michaelandhsm2/big-data-mining-course/blob/master/hw1/HW%20%232.ipynb))
+ Second Iteration (Local) - Plain Scala ([Code](https://github.com/michaelandhsm2/big-data-mining-course/blob/master/hw1/sbt/src/main/scala/hw2.scala))
+ Third Iteration ([Google Dataproc](https://cloud.google.com/dataproc/))
  - 1 Master Node + 3 Worker Node
  - Machine Type: n1-standard-1 (1vCPU, 3.75GB Memory, 10GB Disk)
<!-- ![Cluster Setup Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw1/pics/Cluster%20Setup.PNG) -->
<!--
### Output
- Output File ([Google Drive Download Link](https://drive.google.com/file/d/1Tow0I7p9pmR5fQ41nOS95xFFPLFDQEbX/view?usp=sharing))

- Console Output 1

![Console Output 1 Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw1/pics/Results%20-%201.PNG)

- Console Output 2

![Console Output 2 Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw1/pics/Results%20-%202.PNG) -->
