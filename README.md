# PageRankByHadoop
 Using MapReduce to implement PageRank 

jdk version: 1.8.0_265

hadoop version : 2.9.2

**configuration:**

*Core-site.xml*：

<configuration>

  <property>

​    <name>hadoop.tmp.dir</name>

​    <value>file:/opt/hadoop/tmp</value>

​    <description>Abase for other temporary directories.</description>

  </property>

  <property>

​    <name>fs.defaultFS</name>

​    <value>hdfs://localhost:9000</value>

  </property>

</configuration>

*hdfs-site.xml*：

<configuration>

  <property>

​    <name>dfs.replication</name>

​    <value>1</value>

  </property>

  <property>

​    <name>dfs.namenode.name.dir</name>

​    <value>file:/opt/hadoop/tmp/dfs/name</value>

  </property>

  <property>

​    <name>dfs.datanode.data.dir</name>

​    <value>file:/opt/hadoop/tmp/dfs/data</value>

  </property>

</configuration>

*mapred-site.xml*：

<configuration>

   <property>

​       <name>mapred.job.tracker</name>

​       <value>127.0.0.1:9001</value>

   </property>

</configuration>

**command:**

首先启动hadoop

接着将文件至于hdfs /input目录下:`hadoop fs –put ./ soc-Epinions1.txt /input`

最后分别执行`step1.sh`、`step2_loop.sh`、`step3.sh`