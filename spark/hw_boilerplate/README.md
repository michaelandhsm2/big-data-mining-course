# Homework 1

## Description

### Data
[Individual household electric power consumption dataset](https://archive.ics.uci.edu/ml/datasets/individual+household+electric+power+consumption) - about 2 million instances, 20MB (compressed) in size

### Task
3 subtasks:
+ (30pt) Output the minimum, maximum, and count of the columns: ‘global active power’, ‘global reactive power’, ‘voltage’, and ‘global intensity’
+ (30pt) Output the mean and standard deviation of these columns
+ (40pt) Perform min-max normalization on the columns to generate normalized output

### Implementation Issues
+ Missing values
+ Conversion of data types

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
- Output File ([Google Drive Download Link](https://drive.google.com/file/d/1Tow0I7p9pmR5fQ41nOS95xFFPLFDQEbX/view?usp=sharing))

- Console Output 1

![Console Output 1 Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw1/pics/Results%20-%201.PNG)

- Console Output 2

![Console Output 2 Picture](https://raw.githubusercontent.com/michaelandhsm2/big-data-mining-course/master/hw1/pics/Results%20-%202.PNG)
