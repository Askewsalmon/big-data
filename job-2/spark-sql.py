import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

parser = argparse.ArgumentParser(description="Process input file and save results")
parser.add_argument("--input", help="Path to the input file")
parser.add_argument("--output", help="Path to the output directory")
args = parser.parse_args()

input_file = args.input
output_path = args.output

spark = SparkSession.builder.appName("Job2").getOrCreate()

df = spark.read.csv(input_file, header=True)

df = df.fillna({"industry": "N/A", "sector": "N/A"})

df = (
    df.withColumn("open", col("open").cast("float"))
    .withColumn("close", col("close").cast("float"))
    .withColumn("volume", col("volume").cast("float"))
    .withColumn("year", col("date").substr(1, 4).cast("int"))
)

df.createOrReplaceTempView("stock_data")

result_df = spark.sql(
    """
    WITH industry_year_stats AS (
        SELECT 
            sector,
            industry,
            year,
            sum(open) as total_open,
            sum(close) as total_close,
            sum(volume) as total_volume,
            (sum(close) - sum(open)) / sum(open) * 100 as percentage,
            max((close - open) / open * 100) as max_percentage,
            max(volume) as max_volume
        FROM stock_data
        GROUP BY sector, industry, year
    ),
    max_percentage_tickers AS (
        SELECT
            sector,
            industry,
            year,
            ticker as max_ticker_percentage
        FROM (
            SELECT 
                sector,
                industry,
                year,
                ticker,
                (close - open) / open * 100 as percentage,
                row_number() OVER (PARTITION BY sector, industry, year ORDER BY (close - open) / open * 100 DESC) as rank
            FROM stock_data
        ) ranked_data
        WHERE rank = 1
    ),
    max_volume_tickers AS (
        SELECT
            sector,
            industry,
            year,
            ticker as max_ticker_volume
        FROM (
            SELECT 
                sector,
                industry,
                year,
                ticker,
                volume,
                row_number() OVER (PARTITION BY sector, industry, year ORDER BY volume DESC) as rank
            FROM stock_data
        ) ranked_data
        WHERE rank = 1
    )
    SELECT 
        iys.sector,
        iys.industry,
        iys.year,
        iys.percentage,
        iys.max_percentage,
        mpt.max_ticker_percentage,
        iys.max_volume,
        mvt.max_ticker_volume
    FROM industry_year_stats iys
    JOIN max_percentage_tickers mpt
        ON iys.sector = mpt.sector AND iys.industry = mpt.industry AND iys.year = mpt.year
    JOIN max_volume_tickers mvt
        ON iys.sector = mvt.sector AND iys.industry = mvt.industry AND iys.year = mvt.year
    ORDER BY  iys.percentage DESC
    """
)

result_df.write.mode("overwrite").csv(output_path)

spark.stop()
