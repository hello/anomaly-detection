import matplotlib.pyplot as plt
from matplotlib import cm
from mpl_toolkits.mplot3d import Axes3D
from mpl_toolkits.mplot3d import proj3d
import datetime
from datetime import date
import sys
import itertools
import os
import time

from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn import svm

from fileProcessorWork import *

def get_anomaly_accounts(date):
    fileName = 'labels/' + date + '_jyfan-office.csv'
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
    a = float(sys.argv[1])
    b = float(sys.argv[2])
    targetWindow = 30 
    feature = 'ambient_light'

    accountIdList=['21561','1','1002','1012','1006','1001','1310','1060','1086','1072','1629','1057','1050','26417','1062','1063','1071','33923','1049','15489','1608','1075','41288','25127','27700','42108','38040','42114','44356','29123','1330']
#    accountIdList = ['1']

    dateList = ['2015-11-17','2015-11-16','2015-11-15','2015-11-14','2015-11-13','2015-11-12','2015-11-11', '2015-11-10', '2015-11-09', '2015-11-07', '2015-11-06', '2015-11-05', '2015-11-04', '2015-11-03', '2015-11-02', '2015-11-01'] 
#    dateList = ['2015-11-17','2015-11-16','2015-11-15','2015-11-14','2015-11-13','2015-11-12','2015-11-11', '2015-11-10', '2015-11-09', '2015-11-07', '2015-11-06', '2015-11-05', '2015-11-04', '2015-11-03', '2015-11-02', '2015-11-01', '2015-10-31', '2015-10-30', '2015-10-29', '2015-10-28', '2015-10-27', '2015-10-26', '2015-10-25', '2015-10-24', '2015-10-23', '2015-10-22', '2015-10-21', '2015-10-20', '2015-10-19', '2015-10-18', '2015-10-17', '2015-10-16', '2015-10-15', '2015-10-14', '2015-10-13', '2015-10-12','2015-10-11', '2015-10-10','2015-10-09', '2015-10-08', '2015-10-07','2015-10-6', '2015-10-05', '2015-10-04', '2015-10-03', '2015-10-02','2015-10-01'] 
#    dateList = ['2015-11-11'] 

    i = 0
    labels_all = []
    labels_truth_all = []

    for targetDate in dateList:
        labels_one_day = []
        anomaly_accounts = get_anomaly_accounts(targetDate)
        labels_true = []
        for accountId in accountIdList:
            anomalyFeatList = []
            myFile = 'data/' + accountId + '-' + feature + '-dump.csv'
            normalizedMatrix, reducedDateMatrix = dataProcessor(myFile, targetDate, targetWindow, 0, 6, 6)
            if str(reducedDateMatrix[0]) == str(targetDate):

                eps = getEps(normalizedMatrix) 
                db = DBSCAN(eps=max(a/eps,b), min_samples=5).fit(normalizedMatrix)
                core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
                labels = db.labels_
                
                labels_one_day.append(labels[-1])
                if accountId in anomaly_accounts:
                    labels_true.append(-1)
                    print "anomalous account " + str(accountId) + " on " + str(reducedDateMatrix[-1]) + " DBSCAN: " + str(labels[-1])
                else:
                    labels_true.append(0)
#                    if labels[-1]==-1:
#                        print eps
#                        print "normal account " + str(accountId) + " on " + str(reducedDateMatrix[-1]) + " DBSCAN: " + str(labels[-1])
            else:
                i += 1
                labels_one_day.append(0)
                if accountId in anomaly_accounts:
                    labels_true.append(-2)
                else:
                    labels_true.append(0)

        labels_one_day = np.asarray(labels_one_day)
        labels_true = np.asarray(labels_true)

        labels_all = np.append(labels_all, labels_one_day)
        labels_truth_all = np.append(labels_truth_all, labels_true)

    print a, b
    print metrics.classification_report(labels_truth_all, labels_all)
    print "no sufficient data for " + str(i)
