import csv

import sys
grouped = {}

with open(sys.argv[1]) as fh:
    reader = csv.reader(fh, delimiter='|')
    next(reader, None)
    for row in reader:
        dts, count = row[0].strip(), int(row[1].strip())
        day = dts[:len('2015-11-19 06')]
        if day not in grouped:
            grouped[day] = [0] * 60
        minute = int(dts[len(day)+1:len(day) + 2+1])
        grouped[day][minute] = count

by_day = {}
for k,v in grouped.iteritems():
    day, hour = k.split(' ',2)
    if day not in by_day:
        by_day[day] = [[]] * 7
    by_day[day][int(hour)] = v


with open('clean-' + sys.argv[1], 'w+') as fw:
    writer = csv.writer(fw)
    for k in sorted(by_day.keys()):
        list_of_lists = by_day[k]
        merged = [j for i in list_of_lists for j in i]
        writer.writerow([k] + merged)
