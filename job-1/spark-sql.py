import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    min as spark_min,
    max as spark_max,
    avg,
    first,
    last,
)
from pyspark.sql.types import FloatType, IntegerType, DateType

parser = argparse.ArgumentParser(description="Process input file and save results")
parser.add_argument("--input", help="Path to the input file")
parser.add_argument("--output", help="Path to the output directory")
args = parser.parse_args()

input_file = args.input
output_path = args.output

spark = SparkSession.builder.appName("Job1").getOrCreate()

df = spark.read.csv(input_file, header=True)

df = df.withColumn("date", col("date").cast(DateType())).withColumn(
    "year", col("date").substr(1, 4).cast(IntegerType())
)

df = df.select(
    col("ticker"),
    col("name"),
    col("date"),
    col("low").cast(FloatType()),
    col("high").cast(FloatType()),
    col("volume").cast(FloatType()),
    col("close").cast(FloatType()),
    col("year"),
)

agg_df = df.groupBy("ticker", "year").agg(
    first("name").alias("name"),
    spark_min("low").alias("low"),
    spark_max("high").alias("high"),
    avg("volume").alias("average_volume"),
    first("close").alias("first_close"),
    last("close").alias("last_close"),
    first("date").alias("first_date"),
    last("date").alias("last_date"),
)

result_df = agg_df.withColumn(
    "percentage_change",
    ((col("last_close") - col("first_close")) / col("first_close") * 100).cast(
        FloatType()
    ),
)

final_df = result_df.select(
    "name", "ticker", "year", "low", "high", "average_volume", "percentage_change"
)

final_df.write.mode("overwrite").csv(output_path)

spark.stop()
