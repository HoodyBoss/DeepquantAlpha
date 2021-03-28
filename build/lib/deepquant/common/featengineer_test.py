import numpy
from deepquant.common.featengineer import FeatEngineer as fe

print('============================')
print('Start prepare data...')
actArr = ['OPEN LONG', 'HOLD LONG', 'OPEN SHORT']
rsiArr = [18.9, 32, 38.2, 41.9]

ma1Arr = [90.1, 91.2, 92.3, 93.4]
ma2Arr = [92.5, 93.6, 94.7, 95.8]

sstoKArr = [90.1, 94.2, 95.3, 96.4]
sstoDArr = [92.5, 93.6, 94.7, 95.8]

openArr = [17.8, 29.6, 39.7, 43.5]
closeArr = [18.9, 35.7, 38.2, 41.9]
highArr = [19.1, 36.3, 44.8, 43.9]
lowArr = [11.2, 28.5, 37.9, 40.8]

print('Prepare data successfully....')
print('TEST!....')

print('levelEqually =',fe.levelEqually(99, 100, 10))
print('levelEqually =',fe.levelEqually(1.9, 5, 5))
print('levelEqually =',fe.levelEqually(fe.reverseFromLast(4, rsiArr)[2], 100, 10))
print('levelEquallyAtIndex =',fe.levelEquallyAtIndex(fe.reverseFromLast(4, rsiArr), 100, 10, 2))

print('macdLevelEqually =', fe.macdLevelEqually(1.4, -5, 5, 5))
print('macdLevelEqually =', fe.macdLevelEqually(-4.5, -5, 5, 5))

print('foundHigherThan =', fe.foundHigherThan(fe.reverseFromLast(4, rsiArr), 40, 2))
print('foundLowerThan =', fe.foundLowerThan(fe.reverseFromLast(4, rsiArr), 19, 4))

print('featScaling =', fe.featScaling(993.6, 200, 1200))
print('featScalingWithMaxScal =', fe.featScalingWithMaxScale(993.6, 200, 1200, 100))

print('diff =', fe.diff(100.2, 92.5))
print('diffAtIndex =', fe.diffAtIndex(ma1Arr, ma2Arr, 2))

print('featMax =', fe.featMax(fe.reverseFromLast(4, ma1Arr), 4))
print('featMaxInRange =', fe.featMaxInRange(ma1Arr, 1, 2))
print('featMin =', fe.featMin(ma1Arr, 4))
print('featMinInRange =', fe.featMinInRange(ma1Arr, 1, 2))

print('valLowestDiff =', fe.valLowestDiff(58.7, closeArr, 1, 2))
print('valHighestDiff =', fe.valHighestDiff(58.7, closeArr, 1, 2))

print('featAverage =', fe.featAverage(closeArr, 3))
print('featStDev =', fe.featStDev(closeArr))
print('featStDevInRange =', fe.featStDevInRange(closeArr, 1, 3))

print('foundHigher =', fe.foundHigher(sstoKArr, sstoDArr, 2))
print('foundLower =', fe.foundLower(sstoKArr, sstoDArr, 4))

print('totalHigherThan =', fe.totalHigherThan(sstoKArr, sstoDArr, 4))
print('totalLowerThan =', fe.totalLowerThan(sstoKArr, sstoDArr, 4))

print('foundActionInRange =', fe.foundActionInRange(actArr, 'OPEN LONG', 3))

print('countBullishCds =', fe.countBullishCds(openArr, closeArr, 3))
print('countBearishCds =', fe.countBearishCds(openArr, closeArr, 3))

print('foundLongUpperShadow =', fe.foundLongUpperShadow(highArr, openArr, closeArr, 4, 5.0))
print('foundLongLowerShadow =', fe.foundLongLowerShadow(lowArr, openArr, closeArr, 4, 5.0))

# Test array
featRows = list()
for i in range(0, 4) :
    featCol = list()
    featCol.append(10.1 + i)
    featCol.append(11.2 + i)
    featCol.append(12.3 + i)
    featRows.append(featCol)
    
print(featRows)

featArr = numpy.asarray(featRows)
print(featArr)

featArr[1, 2] = 17.8
print(featArr)
print(featRows)