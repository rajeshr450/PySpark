#!/usr/bin/python
import time
import os
import sys
from pyspark.sql import SparkSession
import logging

sys.path.insert(0, 'mods.zip')
import pytz
 
print('The timezones supported by pytz module: ', pytz.all_timezones, '\n')

from utils.find_count import find_cnt


def main():
    logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    run_logger = logging.getLogger(__name__)
    spark = SparkSession.builder.appName('py3 spark').getOrCreate()
    # spark.sparkContext.setLogLevel("WARN")
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j

    log = log4jLogger.LogManager.getLogger(__name__)

    run_logger.info("Hello World!")

    run_logger.info('This will get logged to a file')
    df = spark.createDataFrame([(1,2),(3,4)],['a','b'])
    df.printSchema()
    count = find_cnt(df, 2)
    if count > 1:
        run_logger.info("count of dataframe is {}".format(count))
    spark.stop()


if __name__ == '__main__':
    main()

