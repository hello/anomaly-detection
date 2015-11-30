import csv
import numpy as np
import datetime
from datetime import date

def readFile(fileName):
    fileMatrix = []
    matrixName = csv.reader(open(fileName))
    for row in matrixName:
        fileMatrix.append(row)
    return fileMatrix

def processData(fileMatrix):
    """
    processes contents of directory labels/* which starts out with columns [ambient_light, month, day, hour, minute] and reshapes to matrix with columns [ambient_light_1, ... , ambient_light_1440] with each row as one day. Saves dates with enough data in dateMatrix. (Removes days without enough data defined as points less than 1420)
    """
    dataMatrix = [[]]
    dataMatrix[0].append(float(fileMatrix[0][0]) )

    if len(fileMatrix[0][2])==1:
        date1 = str(fileMatrix[0][1]) + '-0' + str(fileMatrix[0][2])
    else:
        date1 = str(fileMatrix[0][1]) + '-' + str(fileMatrix[0][2])
    dateMatrix = [date1]
    
    dayIndex = 0
    ptIndex = 1
    while ptIndex < len(fileMatrix):
        if sameDay(fileMatrix[ptIndex-1], fileMatrix[ptIndex]):
            dataMatrix[dayIndex].append(float(fileMatrix[ptIndex][0]))
        else:
            if len(fileMatrix[ptIndex][2])==1:
                date = str(fileMatrix[ptIndex][1]) + '-0' + str(fileMatrix[ptIndex][2])
            else:
                date = str(fileMatrix[ptIndex][1]) + '-' + str(fileMatrix[ptIndex][2])
            dateMatrix.append(date)

            dayIndex = dayIndex + 1
            dataMatrix.append([])
            dataMatrix[dayIndex].append(float(fileMatrix[ptIndex][0]))
        ptIndex = ptIndex + 1

    for rowIndex, row in reversed(list(enumerate(dataMatrix))):
        if 1420 <= len(row) < 1440:
#        if len(row) < 1420:
            row.extend([0]*(1440 - len(row)))

        elif len(row) < 1420:
            dataMatrix.pop(rowIndex)
            dateMatrix.pop(rowIndex)
#    dateMatrix = dateMatrix[:len(dateMatrix)-1]
    return dataMatrix, dateMatrix

def compressData(dataMatrix):
    """
    Takes results of processData() and reshapes into matrix with columns [avg_light from 12-2am, avg_light from 2am-4am, avg_light from 4am-6am] where each row is a day

    """
    compressedMatrix = []
    for index, row in enumerate(dataMatrix[:len(dataMatrix)]):
        list1 = row[0:120]
        list2 = row[120:240]
        list3 = row[240:360]
#        list1 = row[0:129] + dataMatrix[index+1][0:60]
#        list2 = dataMatrix[index+1][60:300]
#        list3 = dataMatrix[index+1][300:540]
                
        sum1 = np.mean(list1) 
        sum2 = np.mean(list2) 
        sum3 = np.mean(list3) 
        
#        sum1 = sum(list1)/(480000*0.5)
#        sum2 = sum(list2)/(480000*20)
#        sum3 = sum(list3)/(480000*10)
        compressedMatrix.append([sum1, sum2, sum3])
    return compressedMatrix

def normalizeData(compressedMatrix):
    normalizedMatrix = []
    compressedMatrix = np.asarray(compressedMatrix)
    avg1 = np.mean(compressedMatrix[:,0]) 
    std1 = np.std(compressedMatrix[:,0])
    avg2 = np.mean(compressedMatrix[:,1]) 
    std2 = np.std(compressedMatrix[:,1])
    avg3 = np.mean(compressedMatrix[:,2]) 
    std3 = np.std(compressedMatrix[:,2])

    for row in compressedMatrix:
        pt1 = (row[0] - avg1 )/ (std1)
        pt2 = (row[1] - avg2 )/ (std2)
        pt3 = (row[2] - avg3 )/ (std3)
        normalizedMatrix.append([pt1, pt2, pt3])
    return np.asarray(normalizedMatrix)

def sameDay(lastRow, newRow):
    if lastRow[2]==newRow[2]:
        return True
    else:
        return False

def reduceDataForDay(date, window, compressedMatrix, dateMatrix):  
    date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    window = int(window)

    dateList = []
    for i in range(0, window+1):
        okDate = date - datetime.timedelta(days=i)
        stringDate = str(okDate.month) + '-' + str(okDate.day)
        dateList.append(stringDate)
#        if okDate.month < 10:
#            dateList.append(str(okDate)[6:])
#        else:
#            dateList.append(str(okDate)[5:])

    reducedMatrix = []
    reducedDateMatrix = []
    for index, dateItem in enumerate(dateMatrix):
        if dateItem in dateList:  
            reducedMatrix.append(compressedMatrix[index])
            reducedDateMatrix.append(dateItem)
    return np.asarray(reducedMatrix), reducedDateMatrix

def dataProcessor(myFile, targetDate, targetWindow):
    fileMatrix = readFile(myFile)
    dataMatrix, dateMatrix = processData(fileMatrix)
    compressedMatrix = compressData(dataMatrix)
    reducedMatrix, reducedDateMatrix = reduceDataForDay(targetDate, targetWindow, compressedMatrix, dateMatrix)
#    return reducedMatrix, reducedDateMatrix
    normalizedMatrix = normalizeData(reducedMatrix)
    return normalizedMatrix, reducedDateMatrix, reducedMatrix

