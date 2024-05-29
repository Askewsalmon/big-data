#!/usr/bin/env python

import sys
from datetime import datetime

results = {}

for line in sys.stdin:
    line = line.strip()

    element = line.split("\t")

    ticker = element[0]
    name = element[1]
    date = element[2]
    low = float(element[3])
    high = float(element[4])
    volume = float(element[5])
    close = float(element[6])

    parse_date = datetime.strptime(date, "%Y-%m-%d")

    year = parse_date.year
    month = parse_date.month
    day = parse_date.day

    if ticker not in results:
        results[ticker] = {"name": name, "statistics": {}}

    if year not in results[ticker]["statistics"]:
        results[ticker]["statistics"][year] = {
            "min_low": low,
            "max_high": high,
            "volumes": [volume],
            "first_close": close,
            "last_close": close,
            "date": date,
        }
    else:
        if low < results[ticker]["statistics"][year]["min_low"]:
            results[ticker]["statistics"][year]["min_low"] = low
        if high > results[ticker]["statistics"][year]["max_high"]:
            results[ticker]["statistics"][year]["max_high"] = high
        if date > results[ticker]["statistics"][year]["date"]:
            results[ticker]["statistics"][year]["last_close"] = close
        if date < results[ticker]["statistics"][year]["date"]:
            results[ticker]["statistics"][year]["first_close"] = close
        results[ticker]["statistics"][year]["volumes"].append(volume)

for ticker, ticker_data in results.items():
    name = ticker_data["name"]
    for year, stats in ticker_data["statistics"].items():
        min_low = stats["min_low"]
        max_high = stats["max_high"]
        avg_volume = round(sum(stats["volumes"]) / len(stats["volumes"]))
        percentage_close = round(
            ((stats["last_close"] - stats["first_close"]) / stats["first_close"]) * 100,
            2,
        )

        print(
            f"{ticker}\t{name}\t{year}\t{min_low}\t{max_high}\t{avg_volume}\t{percentage_close}"
        )
