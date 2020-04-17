import boto3
import ujson
import arrow
import sys
import os
from pyspark.sql import SQLContext
from pyspark import SparkContext

boto3.set_stream_logger('botocore', level='DEBUG')
sc = SparkContext()

version = sys.version
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info("pyspark script logger initialized")
LOGGER.info("Python Version: " + version)

s3_list = []
s3 = boto3.resource('s3')
my_bucket = s3.Bucket('time-waits-for-no-man')
for object in my_bucket.objects.filter(Prefix='1971-01'):
    s3_list.append(object.key)

def add_timestamp(dict):
    dict['timestamp'] = arrow.get(
                        int(dict['year']),
                        int(dict['month']),
                        int(dict['day']),
                        int(dict['hour']),
                        int(dict['minute']),
                        int(dict['second'])
                        ).timestamp
    return dict

def distributedJsonRead(s3Key):
    s3obj = boto3.resource('s3').Object(bucket_name='time-waits-for-no-man', key=s3Key)
    contents = s3obj.get()['Body'].read().decode()
    meow = contents.splitlines()
    result_wo_timestamp = map(ujson.loads, meow)
    result_wi_timestamp = map(add_timestamp, result_wo_timestamp)
    return result_wi_timestamp

sqlContext = SQLContext(sc)
job = sc.parallelize(s3_list)
foo = job.flatMap(distributedJsonRead)
df = foo.toDF()
#df.show()
blah = df.count()
print(blah)
df.printSchema()

#df.write.parquet('dates_by_seconds', mode="overwrite", partitionBy=["second"])
sc.stop()
exit()
