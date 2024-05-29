#!/usr/bin/env python

import sys

is_first_line = True

for line in sys.stdin:
    line = line.strip()
    if is_first_line:
        is_first_line = False
        continue

    words = line.split(",")

    ticker = words[0]
    name = words[9]
    date = words[7]
    low = words[4]
    high = words[5]
    volume = words[6]
    close = words[2]

    print(
        "\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (ticker, name, date, low, high, volume, close)
    )
