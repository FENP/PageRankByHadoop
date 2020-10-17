javac -classpath /opt/hadoop/share/hadoop/common/hadoop-common-2.9.2.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.9.2.jar:/opt/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.9.2.jar:/opt/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar OutLink.java

jar cf step1.jar OutLink*.class

hadoop fs -rm -r /output1

hadoop jar step1.jar OutLink /input /output1

