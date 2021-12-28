import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType
from pyspark.sql import functions as F

spark = SparkSession.\
        builder.\
        appName("folder-stream").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "512m").\
        getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([ \
    StructField("timestamp", TimestampType(), True), \
    StructField("Volume", IntegerType(), True), \
    StructField("Province", StringType(), True), \
    StructField("City", StringType(), True) \
    ])

data = spark \
    .readStream \
    .schema(schema) \
    .csv("crude")

data = data.selectExpr("timestamp", "Volume", "Province") \
    .withColumn('year', F.year("timestamp")) \
    .withColumn('month', F.month("timestamp")) \
    .withColumn('day', F.dayofmonth("timestamp")) \


writing_sink = data.writeStream \
    .partitionBy("year","month","day") \
    .format("json") \
    .option("path", "hdfs://namenode:9000/crudeoil") \
    .option("checkpointLocation", "hdfs://namenode:9000/crudeoil_checkpoint") \
    .option("maxRecordsPerFile", 200) \
    .trigger(processingTime='30 minutes') \
    .start()

writing_sink.awaitTermination()