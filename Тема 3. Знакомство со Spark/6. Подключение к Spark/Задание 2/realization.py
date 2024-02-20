from pyspark.sql import SparkSession
import os
import findspark

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

findspark.init()
findspark.find()

spark = SparkSession \
    .builder \
    .master("yarn") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.cores", 1) \
    .config("spark.driver.cores", 2) \
    .appName("My second session") \
    .getOrCreate()