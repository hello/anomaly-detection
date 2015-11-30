import matplotlib.pyplot as plt
from matplotlib import cm
from mpl_toolkits.mplot3d import Axes3D
from mpl_toolkits.mplot3d import proj3d
import datetime
from datetime import date
import sys
import itertools
import os

from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn import svm

from fileProcessor import *

def loadDictionary(fileName):
    accountDict = {}
    with open('officeDict.csv') as a:
        for line in a:
            (key, val) = line.rstrip('\n').split(',')
            accountDict[str(key)]= str(val)
    return accountDict

def loadLabels(date, account_list):
    fileName = 'labels/' + date + '_jyfan-office.csv'
    anom_account_ids = []
    if os.stat(fileName).st_size !=0:
        with open(fileName) as a:
            for line in a:
                anom_account_ids.append(line.strip('\n'))
    labels_true = []
    for account_id in account_list:
        if str(account_id) in anom_account_ids:
            labels_true.append(-1)
        else:
            labels_true.append(0)
    return np.asarray(labels_true)

def get_anomaly_accounts(date):
    fileName = 'labels/' + date + '_ksg-office.csv'
    anom_list = []
    with open(fileName) as a:
        for line in a:
            anom_list.append(line.strip('\n'))
    return anom_list

def getEps(normalizedMatrix):
    distanceList = []
    for pointPair in itertools.combinations(normalizedMatrix, 2):
        distance = np.linalg.norm(pointPair[0] - pointPair[1])
        distanceList.append(distance)
    percDistance = np.percentile(distanceList, 50)
    return percDistance

if __name__=="__main__":
    targetWindow = str(sys.argv[1])
    alg = 'dbscan'
    accountSet = 'office'

    #calculations for everyone takes forever on my laptop. Option for subset.
    accountIdList=['21561','1','1002','1012','1006','1001','1310','1060','1086','1072','1629','1057','1050','26417','1062','1063','1071','33923','1049','15489','1608','1075','41288','25127','27700','42108','38040','42114','44356','29123','1330']

    #load dictionary for matching accountIds to human-readable names
    accountDict = loadDictionary('officeDict.csv') 

    #list of features to check anomalies for
    featureList = ['ambient_light']

    labels_all = []
    labels_truth_all = []

    dateList = ['2015-11-17','2015-11-16','2015-11-15','2015-11-14','2015-11-13','2015-11-12','2015-11-11', '2015-11-10', '2015-11-09', '2015-11-07', '2015-11-06', '2015-11-05', '2015-11-04', '2015-11-03', '2015-11-02', '2015-11-01'] 
#    dateList = ['2015-11-11'] 
    for targetDate in dateList:

        labels_one_day = []
        anomaly_accounts = get_anomaly_accounts(targetDate)
        labels_true = []
        for accountId in accountIdList:
    #        print 'Crunching for accountId ' + accountId
            anomalyFeatList = []
            for feature in featureList:
                myFile = 'data/' + accountId + '-' + feature + '-dump.csv'
                normalizedMatrix, reducedDateMatrix, reducedMatrix = dataProcessor(myFile, targetDate, targetWindow)
                if reducedDateMatrix[-1] == targetDate[5:]:

                    eps = getEps(normalizedMatrix) 
    #                print eps
                    db = DBSCAN(eps=max(eps,3), min_samples=5).fit(normalizedMatrix)
                    core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
                    labels = db.labels_
                    labels_one_day.append(labels[-1])
                    if accountId in anomaly_accounts:
                        labels_true.append(-1)
                    else:
                        labels_true.append(0)
                else:
                    labels_one_day.append(0)
                    if accountId in anomaly_accounts:
                        labels_true.append(-2)
                    else:
                        labels_true.append(0)

        labels_one_day = np.asarray(labels_one_day)
        labels_true = np.asarray(labels_true)

        labels_all = np.append(labels_all, labels_one_day)
        labels_truth_all = np.append(labels_truth_all, labels_true)

    print metrics.classification_report(labels_truth_all, labels_all)
