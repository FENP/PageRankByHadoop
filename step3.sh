#!/bin/bash
javac -classpath /opt/hadoop/share/hadoop/common/hadoop-common-2.9.2.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.9.2.jar:/opt/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.9.2.jar:/opt/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar SortByPR.java

jar cf step3.jar SortByPR*.class

hadoop fs -rm -r /output3

hadoop jar step3.jar SortByPR /output1 /output3
