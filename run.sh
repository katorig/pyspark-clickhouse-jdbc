#!/bin/bash

ABSOLUTE_PATH_TO_JARFILE = '/home/katorig/clickhouse-jdbc-0.3.1.jar'

SPARK_HOME='/opt/conda/lib/python3.7/site-packages/pyspark' \
spark-submit \
--jars $ABSOLUTE_PATH_TO_JARFILE \
clickhouse_pyspark_connector.py