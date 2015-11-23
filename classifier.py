import csv
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC
from sklearn.metrics import classification_report
import numpy as np

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def read_data(fname):
    days = {}
    with open('tim-pivot.csv') as f:
        reader = csv.reader(f)
        for row in reader:
            days[row[0]] = map(int, row[1:])
    return days


def average_per_hour(sample):
    hour_chunk = chunks(sample, 60)
    res = []
    for chunk in hour_chunk:
        res.append(np.average(chunk))
    return res

def sum_per_hour(sample):
    hour_chunk = chunks(sample, 60)
    res = []
    for chunk in hour_chunk:
        res.append(sum(chunk))
    return res

def min_per_hour(sample):
    hour_chunk = chunks(sample, 60)
    res = []
    for chunk in hour_chunk:
        res.append(min(chunk))
    return res

def max_per_hour(sample):
    hour_chunk = chunks(sample, 60)
    res = []
    for chunk in hour_chunk:
        res.append(max(chunk))
    return res


def global_avg(sample):
    return [np.average(sample)]

def global_sum(sample):
    return [np.sum(sample)]

def global_max(sample):
    return [np.max(sample)]

def global_min(sample):
    return [np.min(sample)]

def perc_75(sample):
    hour_chunk = chunks(sample, 60)
    res = []
    for chunk in hour_chunk:
        res.append(np.percentile(chunk, 75))
    return res

def three_chunks(sample):
    hour_chunk = chunks(sample, 120)
    res = []
    for chunk in hour_chunk:
        res.append(np.average(chunk))
    return res

def do_nothing(sample):
    return sample

def bunch(sample):
    
    _avg = np.average(sample)
    _max = np.max(sample)
    _min = np.min(sample)
    _sum = np.sum(sample)
    _p50 = np.percentile(sample, 50)
    _p75 = np.percentile(sample, 75)
    _p95 = np.percentile(sample, 95)
    return (_avg, _max, _min, _sum, _p50, _p75, _p95)

transformers = {
    'nothing' : do_nothing,
    'average_per_hour' : average_per_hour,
    'sum_per_hour' : sum_per_hour,
    'min_per_hour' : min_per_hour,
    'max_per_hour' : max_per_hour,
    'global_avg' : global_avg,
    'global_sum' : global_sum,
    'global_max' : global_max,
    'global_min' : global_min,
    'perc_75' : perc_75,
    'three_chunks' : three_chunks,
    'bunch' : bunch,
}

train_labels = {
    '2015-11-15' : False,
    '2015-11-14' : False,
    '2015-11-13' : False,
    '2015-11-12' : False,
    '2015-11-11' : False,
    '2015-11-10' : False,
    '2015-11-09' : True,
    '2015-11-08' : False,
    '2015-11-07' : True,
    '2015-11-06' : False,
    '2015-11-05' : False,
    '2015-11-04' : True,
    '2015-11-03' : False,
    '2015-11-02' : False,
    '2015-11-01' : False,
}




test_labels = {
    '2015-11-19' : True,
    '2015-11-18' : False,
    '2015-11-17' : False,
    '2015-11-16' : False,
}

if __name__ == '__main__':
    data = read_data('myfile')

    for name, fn in transformers.iteritems():
        train_keys = sorted(train_labels.keys())
        labels = [train_labels[key] for key in train_keys]

        features = [data[key] for key in train_keys]
        features = map(fn, features)

        test_keys = sorted(test_labels.keys())
        t_labels = [test_labels[key] for key in test_keys]
        t_features = [data[key] for key in test_keys]
        t_features = map(fn, t_features)
        clf = DecisionTreeClassifier()
        clf.fit(features, labels)


        predictions = clf.predict(t_features, t_labels)
        print name
        print classification_report(t_labels, predictions)
        print