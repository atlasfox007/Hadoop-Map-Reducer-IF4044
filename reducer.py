#!/usr/bin/env python3
import sys
import collections

# Shuffling part
d = collections.defaultdict(int)
for line in sys.stdin:
    date, soc_med_type, count = line.strip().split('\t')
    key = (date, soc_med_type)
    val = int(count)

    d[key] += val

# Sorting part
for k, v in sorted(d.items()):
    print(f'{k[0]},{k[1]},{v}')
