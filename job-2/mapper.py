#!/usr/bin /env python

import sys

is_first_line = True

for line in sys.stdin:
    line = line.strip()
    if is_first_line:
        is_first_line = False
        continue

    words = line.split(",")

    ticker = words[0]
    date = words[7]
    volume = words[6]
    open = words[1]
    close = words[2]
    industry = words[11]
    sector = words[10]

    if sector == "":
        sector = "N/A"
    if industry == "":
        industry = "N/A"

    print(
        "\t%s\t%s\t%s\t%s\t%s\t%s\t%s"
        % (ticker, open, close, industry, sector, date, volume)
    )
