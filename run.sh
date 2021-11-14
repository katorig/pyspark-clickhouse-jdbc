#!/bin/bash

SPARK_HOME='/opt/conda/lib/python3.7/site-packages/pyspark' \
spark-submit \
--jars /home/katorig/clickhouse-jdbc-0.3.1.jar \
clickhouse_pyspark_connector.py