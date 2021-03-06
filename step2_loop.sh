#! /bin/bash
javac -classpath /opt/hadoop/share/hadoop/common/hadoop-common-2.9.2.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.9.2.jar:/opt/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.9.2.jar:/opt/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar ComputePR.java

jar cf step2.jar ComputePR*.class
for i in {1..10}
do
	echo $i
	hadoop fs -rm -r /output2
	hadoop jar step2.jar ComputePR /output1 /output2
	hadoop fs -rm -r /output1
	hadoop fs -mv /output2 /output1
done
