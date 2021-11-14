#!/usr/bin/env python

import os
from pyspark.sql import SparkSession

ABSOLUTE_PATH_TO_JARFILE = '/home/katorig/clickhouse-jdbc-0.3.1.jar'
CLICKHOUSE_JAR = f'file://{ABSOLUTE_PATH_TO_JARFILE}'


def init_spark(app_name: str, num_executors: int, executor_memory='1G',
               executor_cores=2, driver_memory='2G', queue=''):

    spark_session = (
        SparkSession
        .builder
        .appName(app_name)
        .master('yarn')
        .config('spark.yarn.queue', queue)
        .config('spark.driver.memory', driver_memory)
        .config('spark.executor.cores', executor_cores)
        .config('spark.executor.memory', executor_memory)
        .config('spark.executor.instances', num_executors)  # add more parameters if needed
        .config('spark.driver.userClassPathFirst', 'true')
        .config('spark.driver.extraLibraryPath',
                f'/usr/hdp/2.6.5.0-292/hadoop/lib/native:{CLICKHOUSE_JAR}')  # example for multiple paths
        .config('spark.executor.userClassPathFirst', 'true')
        .config('spark.executor.extraLibraryPath',
                f'/usr/hdp/2.6.5.0-292/hadoop/lib/native:{CLICKHOUSE_JAR}')
    )

    spark = (
        spark_session
        .getOrCreate()
    )

    return spark


spark = init_spark("click-katorig", queue='data', num_executors=3)

host = os.environ.get('CH_HOST')
database = os.environ.get('CH_DATABASE')
port = os.environ.get('CH_PORT')
url = f'jdbc:clickhouse://{host}:{port}/{database}'
user = os.environ.get('CH_USERNAME')
password = os.environ.get('CH_PASSWORD')
dbtable = f'{database}.table'
driver = 'ru.yandex.clickhouse.ClickHouseDriver'

df = (
    spark.read.format('jdbc')
    .option('driver', driver)
    .option('url', url)
    .option('user', user)
    .option('password', password)
    .option('dbtable', dbtable)
    .load()
)

print(df.show(5))

spark.stop()
