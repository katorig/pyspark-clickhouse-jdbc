# Connection to Clickhouse in Pyspark using JDBC Driver

Python 3.7 \
Clickhouse JDBC Driver 0.3.1


This repository contains example of connection to Clickhouse database in Pyspark application. 
JAR file from https://mvnrepository.com/artifact/ru.yandex.clickhouse/clickhouse-jdbc was used, version 0.3.1

`run.sh`  for running `clickhouse_pyspark_connector.py` \
`notebook_example.ipynb` for running Jupyter notebook on hadoop cluster. 

Environment variables and archives paths needed for running application on particular hadoop cluster are missed on purpose.

Note: don't forget to edit path to JAR file (ABSOLUTE_PATH_TO_JARFILE variable in all files). Also, JAR file can be located in hdfs repository.
