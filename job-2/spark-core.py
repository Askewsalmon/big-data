#!/usr/bin/env python

from pyspark import SparkConf, SparkContext
import argparse
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, help="Input file path")
parser.add_argument("--output", type=str, help="Output file path")
args = parser.parse_args()
input_file, output_path = args.input, args.output

conf = SparkConf().setAppName("Job2")
sc = SparkContext(conf=conf)


def parse_line(line):
    words = line.split(",")
    ticker = words[0]
    open_price = float(words[1])
    close = float(words[2])
    industry = words[11]
    date = words[7]
    year = date.split("-")[0]
    volume = float(words[6])
    sector = words[10]

    if industry == "":
        industry = "N/A"
    if sector == "":
        sector = "N/A"
    return (sector, industry, year, ticker, open_price, close, volume)
    total_open = ticker_data[0]
    total_close = ticker_data[1]
    total_volume = ticker_data[2]
    percentage = round(((total_close - total_open) / total_open) * 100, 2)
    return (total_volume, total_open, total_close, percentage)


def process_industry_data(records):
    industry_stats = {}
    max_values = {}

    for record in records:
        sector, industry, year, ticker, open_price, close, volume = record

        if industry not in industry_stats:
            industry_stats[industry] = {}

        if year not in industry_stats[industry]:
            industry_stats[industry][year] = {
                "total_open": 0,
                "total_close": 0,
                "total_volume": 0,
                "max_percentage": -float("inf"),
                "max_ticker_percentage": "",
                "max_volume": 0,
                "max_ticker_volume": "",
            }

        industry_stats[industry][year]["total_open"] += open_price
        industry_stats[industry][year]["total_close"] += close
        industry_stats[industry][year]["total_volume"] += volume

        percentage = (close - open_price) / open_price * 100
        if percentage > industry_stats[industry][year]["max_percentage"]:
            industry_stats[industry][year]["max_percentage"] = percentage
            industry_stats[industry][year]["max_ticker_percentage"] = ticker

        if volume > industry_stats[industry][year]["max_volume"]:
            industry_stats[industry][year]["max_volume"] = volume
            industry_stats[industry][year]["max_ticker_volume"] = ticker

    return industry_stats


def format_output(sector, industry_stats):
    results = []
    for industry, years in industry_stats.items():
        for year, stats in years.items():
            result = (
                sector,
                industry,
                year,
                stats["max_percentage"],
                stats["max_ticker_percentage"],
                stats["max_volume"],
                stats["max_ticker_volume"],
            )
            results.append(result)
    return results


lines = sc.textFile(input_file)
header = lines.first()
data = (
    lines.filter(lambda x: x != header).map(parse_line).filter(lambda x: x is not None)
)

grouped_data = data.groupBy(lambda x: x[0])

formatted_data = grouped_data.mapValues(process_industry_data).flatMap(
    lambda x: format_output(x[0], x[1])
)

formatted_data.saveAsTextFile(output_path)

sc.stop()
