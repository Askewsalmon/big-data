#!/usr/bin/env python

import sys
from datetime import datetime

results = {}

for line in sys.stdin:
    line = line.strip()

    element = line.split("\t")

    ticker = element[0]
    open = float(element[1])
    close = float(element[2])
    industry = element[3]
    sector = element[4]
    date = element[5]
    volume = float(element[6])

    parse_date = datetime.strptime(date, "%Y-%m-%d")

    year = parse_date.year

    if sector not in results:
        results[sector] = {"sector": sector, "statistics": {}, "max_values": {}}
    if industry not in results[sector]["statistics"]:
        results[sector]["statistics"][industry] = {}
    if industry not in results[sector]["max_values"]:
        results[sector]["max_values"][industry] = {
            "max_percentage": -float("inf"),
            "max_ticker_percentage": "",
            "max_volume": 0,
            "max_ticker_volume": "",
        }
    if year not in results[sector]["statistics"][industry]:
        results[sector]["statistics"][industry][year] = {
            "tickers": {},
            "total_volume": 0,
            "total_open": 0,
            "total_close": 0,
        }
    if ticker not in results[sector]["statistics"][industry][year]["tickers"]:
        results[sector]["statistics"][industry][year]["tickers"][ticker] = {
            "open": open,
            "close": close,
            "volume": volume,
        }
    else:
        results[sector]["statistics"][industry][year]["tickers"][ticker][
            "volume"
        ] += volume

    results[sector]["statistics"][industry][year]["total_volume"] += volume
    results[sector]["statistics"][industry][year]["total_open"] += open
    results[sector]["statistics"][industry][year]["total_close"] += close

for sector, sector_data in results.items():
    for industry, industry_data in sector_data["statistics"].items():
        max_percentage = -float("inf")
        max_volume = 0
        max_ticker_percentage = ""
        max_ticker_volume = ""
        for year, stats in industry_data.items():
            total_volume = stats["total_volume"]
            total_open = stats["total_open"]
            total_close = stats["total_close"]
            percentage = round(((total_close - total_open) / total_open) * 100, 2)
            stats["percentage"] = percentage

            for ticker, ticker_stats in stats["tickers"].items():
                ticker_open = ticker_stats["open"]
                ticker_close = ticker_stats["close"]
                ticker_volume = ticker_stats["volume"]
                ticker_percentage = round(
                    ((ticker_close - ticker_open) / ticker_open) * 100, 2
                )

                if ticker_percentage > max_percentage:
                    max_percentage = ticker_percentage
                    max_ticker_percentage = ticker
                if ticker_volume > max_volume:
                    max_volume = ticker_volume
                    max_ticker_volume = ticker

        sorted_industry_data = sorted(
            industry_data.items(), key=lambda x: x[1]["percentage"], reverse=True
        )
        results[sector]["statistics"][industry] = dict(sorted_industry_data)

        results[sector]["max_values"][industry]["max_percentage"] = max_percentage
        results[sector]["max_values"][industry][
            "max_ticker_percentage"
        ] = max_ticker_percentage
        results[sector]["max_values"][industry]["max_volume"] = max_volume
        results[sector]["max_values"][industry]["max_ticker_volume"] = max_ticker_volume


for sector, sector_data in results.items():
    for industry, industry_data in sector_data["statistics"].items():
        max_values = sector_data["max_values"][industry]
        for year, stats in industry_data.items():
            print(
                f"{sector}\t{industry}\t{year}\t{stats['percentage']}\t{max_values['max_ticker_percentage']}\t{max_values['max_percentage']}\t{max_values['max_ticker_volume']}\t{max_values['max_volume']}"
            )
