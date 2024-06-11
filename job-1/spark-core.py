#!/usr/bin/env python

from pyspark import SparkConf, SparkContext
import argparse
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, help="Input file path")
parser.add_argument("--output", type=str, help="Output file path")
args = parser.parse_args()
input_file, output_path = args.input, args.output

conf = SparkConf().setAppName("Job1")
sc = SparkContext(conf=conf)


def parse_line(line):
    words = line.split(",")
    ticker = words[0]
    name = words[9]
    date = words[7]
    low = float(words[4])
    high = float(words[5])
    volume = float(words[6])
    close = float(words[2])
    return (ticker, (name, date, low, high, volume, close))


def process_records(records):
    year_data = {}
    for record in records:
        name, date, low, high, volume, close = record
        parse_date = datetime.strptime(date, "%Y-%m-%d")
        year = parse_date.year

        if year not in year_data:
            year_data[year] = {
                "name": name,
                "low": low,
                "high": high,
                "volumes": [volume],
                "open": close,
                "close": close,
                "first_date": date,
                "last_date": date,
            }
        else:
            year_data[year]["low"] = min(low, year_data[year]["low"])
            year_data[year]["high"] = max(high, year_data[year]["high"])
            year_data[year]["volumes"].append(volume)
            if date < year_data[year]["first_date"]:
                year_data[year]["open"] = close
                year_data[year]["first_date"] = date
            if date > year_data[year]["last_date"]:
                year_data[year]["close"] = close
                year_data[year]["last_date"] = date

    result = []
    for year, data in year_data.items():
        avg_volume = round(sum(data["volumes"]) / len(data["volumes"]), 2)
        annual_return = round((data["close"] - data["open"]) / data["open"] * 100, 2)
        result.append(
            (data["name"], year, data["low"], data["high"], avg_volume, annual_return)
        )

    return result


lines = sc.textFile(input_file)
header = lines.first()
data = lines.filter(lambda x: x != header).map(parse_line)

grouped_rdd = data.groupByKey()

results = grouped_rdd.flatMap(lambda x: process_records(x[1]))

results.saveAsTextFile(output_path)

sc.stop()
