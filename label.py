import csv
import matplotlib.pyplot as plt
from math import sqrt
import sys

def read_data(fname):
    days = {}
    with open(sys.argv[1]) as f:
        reader = csv.reader(f)
        for row in reader:
            days[row[0]] = map(sqrt, map(int, row[1:]))
    return days





data = read_data('what')
print data.keys()
fig = plt.figure()

ax = fig.add_subplot(1,1,1)
for k, v in data.iteritems():



    # for key,values in acc.iteritems():
    # ax.plot(range(len(acc['normal'])), acc['normal'], label='normal')
    ax.plot(range(len(v)), v, label=k)
ax.legend()
plt.savefig('october' + '.png')
