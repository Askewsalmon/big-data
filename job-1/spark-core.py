#!/usr/bin/env python

from pyspark import SparkConf, SparkContext
import argparse
from datetime import datetime
from collections import defaultdict

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
    year_data = []
    for record in records:
        ticker, value = record
        name, date, low, high, volume, close = value
        parse_date = datetime.strptime(date, "%Y-%m-%d")
        year = parse_date.year

        if not year_data or year_data[-1][0] != year:
            year_data.append(
                (
                    name,
                    year,
                    low,
                    high,
                    [volume],
                    close,
                    close,
                    date,
                )
            )
        else:
            last_year_data = year_data[-1]
            if date > last_year_data[6]:
                year_data[-1] = (
                    name,
                    year,
                    min(low, last_year_data[1]),
                    max(high, last_year_data[2]),
                    last_year_data[3] + [volume],
                    close,
                    last_year_data[5],
                    date,
                )
            if date < last_year_data[6]:
                year_data[-1] = (
                    name,
                    year,
                    min(low, last_year_data[1]),
                    max(high, last_year_data[2]),
                    last_year_data[3] + [volume],
                    last_year_data[4],
                    close,
                    last_year_data[6],
                )

    for i in range(len(year_data)):
        year_data[i] = (
            year_data[i][0],
            year_data[i][1],
            year_data[i][2],
            year_data[i][3],
            round(sum(year_data[i][4]) / len(year_data[i][4]), 2),
            round((year_data[i][5] - year_data[i][6]) / year_data[i][6] * 100, 2),
        )

    return year_data


lines = sc.textFile(input_file)
header = lines.first()
data = lines.filter(lambda x: x != header).map(parse_line)

print(data.take(5))

grouped_rdd = data.groupBy(lambda x: (x[0]))

results = grouped_rdd.flatMapValues(process_records)

for result in results.take(5):
    print(result)

results.saveAsTextFile(output_path)

sc.stop()
