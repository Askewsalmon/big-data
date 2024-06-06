import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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

df.createOrReplaceTempView("stock_data")

result_df = spark.sql(
    """
    SELECT 
        ticker,
        year,
        FIRST(name) as name,
        MIN(low) as low,
        MAX(high) as high,
        AVG(volume) as average_volume,
        FIRST(close) as first_close,
        LAST(close) as last_close,
        ((LAST(close) - FIRST(close)) / FIRST(close) * 100) as percentage_change
    FROM stock_data
    GROUP BY ticker, year
"""
)

final_df = result_df.select(
    "name", "ticker", "year", "low", "high", "average_volume", "percentage_change"
)

final_df.write.mode("overwrite").csv(output_path)

spark.stop()
