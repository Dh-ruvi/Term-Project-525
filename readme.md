# Term Project using Python for Apache Spark

## to Run

```bash

./TermProject.py hdfs://localhost:9000/user/student/airline/1999.csv hdfs://localhost:9000/tmp/output

```

## start-hdfs.bash

```bash
#!/bin/bash
cd /usr/local/hadoop/sbin
rm -rf /tmp/hadoop-student
hdfs namenode -format
./start-all.sh
```

## prepare hdfs data

```bash
#!/bin/bash
hdfs dfs -mkdir -p /user/student/airline
hdfs dfs -mkdir -p /user/student/shakespeare
hdfs dfs -mkdir /tmp
hdfs dfs -copyFromLocal ./airline/* /user/student/airline
hdfs dfs -copyFromLocal ./shakespeare/tragedy /user/student/shakespeare
```
