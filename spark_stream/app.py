import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, TimestampType, FloatType, StringType
from pyspark.sql import functions as F


if __name__ == '__main__':
#     try:
    spark = SparkSession\
            .builder\
            .appName("Crude Streaming 2")\
            .master("spark://spark-master:7077")\
            .config("spark.executor.memory", "512m")\
            .getOrCreate()
    
    crude = StructType([StructField("timestamp", TimestampType(), True),
                        StructField("value", FloatType(), True)])

    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "broker:9092") \
      .option("subscribe", "queueing.crude") \
      .option("startingOffsets", "earliest") \
      .option("failOnDataLoss", "false") \
      .load()

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumn("value_str", F.regexp_replace("value",'\\"', '')) \
    .withColumn("timestamp_str", F.split(F.split("value_str", ",")[0],":")[1]) \
    .withColumn("price_str", F.split(F.split("value_str", ", ")[1],":")[1]) \
    .withColumn("price", F.trim(F.regexp_replace("price_str",'}', ''))) \
    .withColumn("timestamp_value", F.to_timestamp("timestamp_str")) \

    writing_sink = df.selectExpr("timestamp_value", "price") \
        .withColumn('year', F.year("timestamp_value")) \
        .withColumn('month', F.month("timestamp_value")) \
        .withColumn('day', F.dayofmonth("timestamp_value")) \
        .writeStream.partitionBy("year","month","day") \
        .format("json") \
        .option("path", "hdfs://namenode:9000/SRC0003") \
        .option("checkpointLocation", "hdfs://namenode:9000/SRC0003_checkpoint") \
        .option("maxRecordsPerFile", 200) \
        .trigger(processingTime='30 minutes') \
        .start()

    writing_sink.awaitTermination()