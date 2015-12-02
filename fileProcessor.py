import csv
import numpy as np
import datetime
from datetime import date

def readFileToDict(fileName):
    allDataDict = {}
    reader = csv.reader(open(fileName))
    for row in reader:
        ambient_light = row[0]
        date = str(row[1]) + '-' + str(row[2])
        hour = int(row[3])
        minute = int(row[4])
        allDataDict[(date, hour, minute)] = ambient_light
    return allDataDict

def getDataForDay(month, day, allDataDict, beginHr, endHr):
    targetDate = str(month) + '-' + str(day)

    dayData = []
    for key, val in allDataDict.iteritems():
        (date, hour, minute) = key
        if date == targetDate and hour >= beginHr and hour < endHr:
            dayData.append(float(val))
    if len(dayData) >= (endHr - beginHr)*60 - 20:
        return dayData
    else:
        return [] 

def processData(allDataDict, targetDate, window, beginHr, endHr):
    """
    processes contents of directory labels/* which starts out with columns [ambient_light, month, day, hour, minute] and reshapes to matrix with columns [ambient_light_1, ... , ambient_light_360] with each row as one day. Saves dates with enough data in dateMatrix. (Removes days without enough data defined as points less than 360 pts between 0 to 6am)
    """
    targetDate = datetime.datetime.strptime(targetDate, "%Y-%m-%d").date()
    window = int(window)

    dateList = []
    for i in range(0, window+1):
        okDate = targetDate - datetime.timedelta(days=i)
        dateList.append(okDate)

    dataMatrix = []
    dateListReduced = []
    for date in dateList:
        month = date.month
        day = date.day
        dayData = getDataForDay(month, day, allDataDict, beginHr, endHr)
        if not dayData:
            print "date skipped for insufficient data: " + str(date)
            continue
        dataMatrix.append(dayData)     
        dateListReduced.append(date)
    return dataMatrix, dateListReduced

def compressData(dataMatrix, num_features):
    """
    Takes results of processData() and reshapes into matrix with columns [avg_light from 12-2am, avg_light from 2am-4am, avg_light from 4am-6am] where each row is a day
    TODO: generalize for arbitrary num_features
    """
    compressedMatrix = []
    for index, row in enumerate(dataMatrix[:len(dataMatrix)-1]):
        list1 = row[0:60]
        list2 = row[60:120]
        list3 = row[120:180]
        list4 = row[180:240]
        list5 = row[240:320]
        list6 = row[320:380]
                
        sum1 = np.mean(list1) 
        sum2 = np.mean(list2) 
        sum3 = np.mean(list3) 

        sum4 = np.mean(list4)
        sum5 = np.mean(list5)
        sum6 = np.mean(list6)
        
#        sum1 = sum(list1)/(480000*0.5)
#        sum2 = sum(list2)/(480000*20)
#        sum3 = sum(list3)/(480000*10)
#        compressedMatrix.append([sum1, sum2, sum3])
        compressedMatrix.append([sum1, sum2, sum3, sum4, sum5, sum6])
    return compressedMatrix

def normalizeData(compressedMatrix):
    normalizedMatrix = []
    compressedMatrix = np.asarray(compressedMatrix)
    if len(compressedMatrix) == 0:
        return np.asarray([])

    normalizedMatrix = (compressedMatrix - compressedMatrix.mean(axis=0))/ compressedMatrix.std(axis=0)
    return np.asarray(normalizedMatrix)

def dataProcessor(myFile, targetDate, targetWindow, beginHr, endHr, num_buckets):
    fileDict = readFileToDict(myFile)
    dataMatrix, dateMatrix = processData(fileDict, targetDate, targetWindow, beginHr, endHr)
    compressedMatrix = compressData(dataMatrix, num_buckets)
    normalizedMatrix = normalizeData(compressedMatrix)
    return normalizedMatrix, dateMatrix 

