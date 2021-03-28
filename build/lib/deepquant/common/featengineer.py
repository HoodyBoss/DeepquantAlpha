import math


# Class version 1
# Last modified date 28/11/2017
# Modified by PENG
class FeatEngineer:

    # ==================================================================================================================
    # FeatureEngineer: คือโมดูล utility สำหรับช่วย extract feature ในการทำ feature engineering
    # ==================================================================================================================
    @staticmethod
    def reverse(startRow, maxBackwardRow, arr):
        startIndex = (startRow + 1) - maxBackwardRow
        endIndex = startRow + 1
        newArr = arr[startIndex: endIndex]
        return newArr[::-1]

    @staticmethod
    def reverseFromLast(maxBackwardRow, arr):
        newArr = arr[(len(arr)) - maxBackwardRow: len(arr)]
        return newArr[::-1]

    # =================================================================================================================
    # Return ค่า val ที่ปรับสเกลแล้ว ค่าใหม่จะอยู่ในปช่วง 0 - 1
    # minVal คือค่าต่ำที่สุดจากจำนวนทั้งหมด maxVal คือค่าสูงสุดจากจำนวนทั้งหมด
    # =================================================================================================================
    @staticmethod
    def featScaling(val, minVal, maxVal):
        return (val - minVal) / (maxVal - minVal)

    # =================================================================================================================
    # Return ค่า val ที่ปรับสเกลแล้ว ค่าใหม่จะอยู่ในปช่วง 0 - maxScale
    # minVal คือค่าต่ำที่สุดจากจำนวนทั้งหมด maxVal คือค่าสูงสุดจากจำนวนทั้งหมด
    # เช่น maxScale = 100 ค่าใหม่จะอยู่ในช่วง 0 - 100
    # =================================================================================================================
    @staticmethod
    def featScalingWithMaxScale(val, minVal, maxVal, maxScale):
        return ((val - minVal) / (maxVal - minVal)) * maxScale

    # =================================================================================================================
    # Return ค่าที่หักลบกัน โดยใช้ val1 - val2
    # =================================================================================================================
    @staticmethod
    def diff(val1, val2):
        return val1 - val2

    # =================================================================================================================
    # Return ค่าที่หักลบกัน โดยใช้ arr1[indexPosition] - arr2[indexPosition]
    # =================================================================================================================
    @staticmethod
    def diffAtIndex(arr1, arr2, indexPosition):
        result = arr1[indexPosition] - arr2[indexPosition]
        return result

    # =================================================================================================================
    # Return หมายเลขที่ใช้แทนสีของแท่งเทียน: 1 (green/bullish), 2 (red/bearish), 3 (no color, priceOpen = priceClose)
    # =================================================================================================================
    @staticmethod
    def cdsColor(priceOpen, priceClose):
        if (priceClose > priceOpen):
            return int(1)
        elif (priceOpen > priceClose):
            return int(2)
        else:
            return int(3)

    # =================================================================================================================
    # Return ค่า absolute value ของ priceOpen - priceClose
    # =================================================================================================================
    # ไม่มี function abs ใน math
    @staticmethod
    def cdsSize(priceOpen, priceClose):
        return math.abs(priceOpen - priceClose)

    # =================================================================================================================
    # Return ค่าสูงสุดใน arr ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange
    # data type ของ arr คือ array ที่ reverse ลำดับมาแล้ว
    # =================================================================================================================
    @staticmethod
    def featMax(arr, maxRange):
        maxVal = arr[0]
        for i in range(0, maxRange):
            maxVal = max(maxVal, arr[i])
        return maxVal

    # =================================================================================================================
    # Return ค่าสูงสุดใน arr ตั้งแต่ตำแหน่ง startIndex ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange
    # Example: x = [1.1, 2.2, 3.3, 4.4]
    # first element (index 0) = 4.4, last element (index 3) = 1.1
    # featMaxInRange(x, 0, 4) = 4.4
    # featMaxInRange(x, 1, 3) = 3.3
    # featMaxInRange(x, 2, 2) = 2.2
    # =================================================================================================================
    @staticmethod
    def featMaxInRange(arr, startIndex, maxRange):
        maxVal = arr[startIndex]
        for i in range(startIndex, (startIndex + maxRange)):
            if (i >= startIndex):
                maxVal = max(maxVal, arr[i])
        return maxVal

    # =================================================================================================================
    # Return ค่าต่ำสุดใน arr ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange
    # =================================================================================================================
    @staticmethod
    def featMin(arr, maxRange):
        minVal = arr[0]
        for i in range(0, maxRange):
            minVal = min(minVal, arr[i])
        return minVal

    # =================================================================================================================
    # Return ค่าต่ำสุดใน arr ตั้งแต่ตำแหน่ง startIndex ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange
    # Example: x = [1.1, 2.2, 3.3, 4.4]
    # first element (index 0) = 4.4, last element (index 3) = 1.1
    # featMinInRange(x, 0, 4) = 1.1
    # featMinInRange(x, 1, 3) = 1.1
    # featMinInRange(x, 1, 2) = 2.2
    # =================================================================================================================
    @staticmethod
    def featMinInRange(arr, startIndex, maxRange):
        minVal = arr[startIndex]
        for i in range(startIndex, (startIndex + maxRange)):
            if (i >= startIndex):
                minVal = min(minVal, arr[i])
        return minVal

    # =================================================================================================================
    # Return ค่าเฉลี่ยใน arr ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange
    # =================================================================================================================
    @staticmethod
    def featAverage(arr, maxRange):
        avgVal = 0.0
        sumVal = 0.0
        for i in range(0, maxRange):
            sumVal = sumVal + arr[i]

        avgVal = sumVal / maxRange
        return avgVal

    # =================================================================================================================
    # Return ค่าส่วนเบี่ยงเบนมาตรฐานใน arr ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange
    # =================================================================================================================
    @staticmethod
    def featStDevInRange(arr, startIndex, maxRange):
        result = 0.0
        avgVal = 0.0
        sumVal = 0.0

        for i in range(startIndex, (startIndex + maxRange)):
            if (i >= startIndex):
                sumVal = sumVal + arr[i]

        avgVal = sumVal / maxRange
        avgInner = 0.0
        sumInner = 0.0

        for i in range(startIndex, (startIndex + maxRange)):
            if (i >= startIndex):
                e = pow(arr[i] - avgVal, 2)
                sumInner = sumInner + e

        avgInner = sumInner / maxRange
        result = math.sqrt(avgInner)
        return result

    # =================================================================================================================
    # Return ค่าส่วนเบี่ยงเบนมาตรฐานใน arr
    # =================================================================================================================
    @staticmethod
    def featStDev(arr):
        return FeatEngineer.featStDevInRange(arr, 0, len(arr))

    # =================================================================================================================
    # Return ค่าที่หักลบกัน โดยใช้ value ลบกับค่าต่ำสุดที่พบใน arr ตั้งแต่ตำแหน่งที่ startIndex ย้อนหลังไป maxRange
    # เช่น
    # closeArr = [18.9, 35.7, 38.2, 41.9]
    # value = 58.7
    # valLowestDiff(value, closeArr, 1, 2) = 23 # 58.7 - 35.7 = 23
    # =================================================================================================================
    @staticmethod
    def valLowestDiff(value, arr, startIndex, maxRange):
        lowest = FeatEngineer.featMinInRange(arr, startIndex, maxRange)
        result = value - lowest
        return result

    # =================================================================================================================
    # Return ค่าที่หักลบกัน โดยใช้ ค่าสูงสุดที่พบใน arr ตั้งแต่ตำแหน่งที่ startIndex ย้อนหลังไป maxRange ลบกับ value
    # เช่น
    # closeArr = [18.9, 35.7, 38.2, 41.9]
    # value = 20.4
    # valHighestDiff(value, closeArr, 0, 3) = 21.5 # 41.9 - 20.4 = 21.5
    # =================================================================================================================
    @staticmethod
    def valHighestDiff(value, arr, startIndex, maxRange):
        highest = FeatEngineer.featMaxInRange(arr, startIndex, maxRange)
        result = highest - value
        return result


    # =================================================================================================================
    # Return 1 เมื่อพบค่าใน arr ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange มีค่าสูงกว่า value, ถ้าไม่พบ return 0
    # เช่น rsiArr = [72.1, 71, 69.8, 68]
    # foundHigherThan(rsiArr, 70, 4) = 1
    # foundHigherThan(rsiArr, 70, 2) = 0
    # =================================================================================================================
    @staticmethod
    def foundHigherThan(arr, value, maxRange):
        found = 0
        for i in range(0, maxRange):
            if (arr[i] > value):
                found = 1
                break
        return int(found)

    # =================================================================================================================
    # Return 1 เมื่อพบค่าใน arr ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange มีค่าต่ำกว่า value, ถ้าไม่พบ return 0
    # เช่น rsiArr = [64.3, 64.8, 66.9, 68]
    # foundLowerThan(rsiArr, 70, 4) = 1
    # foundLowerThan(rsiArr, 65, 2) = 0
    # =================================================================================================================
    @staticmethod
    def foundLowerThan(arr, value, maxRange):
        found = 0
        for i in range(0, maxRange):
            if (arr[i] < value):
                found = 1
                break
        return int(found)

    # =================================================================================================================
    # Return 1 เมื่อพบค่าใน arr1 ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange มีค่าสูงว่าค่าใน arr2, ถ้าไม่พบ return 0
    # เช่น rsiArr1 = [64.3, 64.8, 66.9, 68]
    # rsiArr2 = [63.1, 65.6, 67.8, 68.2]
    # foundHigher(rsiArr1, rsiArr2, 4) = 1
    # foundHigher(rsiArr1, rsiArr2, 2) = 0
    # =================================================================================================================
    @staticmethod
    def foundHigher(arr1, arr2, maxRange):
        found = 0
        for i in range(0, maxRange):
            if (arr1[i] > arr2[i]):
                found = 1
                break
        return int(found)

    # =================================================================================================================
    # Return 1 เมื่อพบค่าใน arr1 ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange มีค่าต่ำว่าค่าใน arr2, ถ้าไม่พบ return 0
    # เช่น rsiArr1 = [64.3, 64.8, 66.9, 68]
    # rsiArr2 = [64.8, 63.2, 62.5, 61.7]
    # foundLower(rsiArr1, rsiArr2, 4) = 1
    # foundLower(rsiArr1, rsiArr2, 2) = 0
    # =================================================================================================================
    @staticmethod
    def foundLower(arr1, arr2, maxRange):
        found = 0
        for i in range(0, maxRange):
            if (arr1[i] < arr2[i]):
                found = 1
                break
        return int(found)

    # =================================================================================================================
    # Return 1 เมื่อพบค่า val1 มากกว่า val2, ถ้าไม่พบ return 0
    # =================================================================================================================
    @staticmethod
    def foundHigherValue(val1, val2):
        if (val1 > val2):
            return int(1)
        else:
            return int(0)

    # =================================================================================================================
    # Return 1 เมื่อพบค่า val1 น้อยกว่า val2, ถ้าไม่พบ return 0
    # =================================================================================================================
    @staticmethod
    def foundLowerValue(val1, val2):
        if (val1 < val2):
            return int(1)
        else:
            return int(0)

    # =================================================================================================================
    # Return 1 เมื่อพบค่าใน tradeActionNames ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange มีค่าตรงกับ targetActionName, ถ้าไม่พบ return 0
    # =================================================================================================================
    @staticmethod
    def foundActionInRange(tradeActionNames, targetActionName, maxRange):
        found = 0
        for i in range(0, maxRange):
            if (tradeActionNames[i] == targetActionName):
                found = 1
                break
        return int(found)

    # =================================================================================================================
    # Return จำนวนแท่งเทียนสีเขียว (bullish) ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange
    # =================================================================================================================
    @staticmethod
    def countBullishCds(priceOpens, priceCloses, maxRange):
        total = 0
        for i in range(0, maxRange):
            if (priceCloses[i] > priceOpens[i]):
                total = total + 1
        return int(total)

    # =================================================================================================================
    # Return จำนวนแท่งเทียนสีแดง (bearish) ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange
    # =================================================================================================================
    @staticmethod
    def countBearishCds(priceOpens, priceCloses, maxRange):
        total = 0
        for i in range(0, maxRange):
            if (priceOpens[i] > priceCloses[i]):
                total = total + 1
        return int(total)

    # =================================================================================================================
    # Return จำนวนแท่งเทียน/เซสชั่น ของอิลิเม้นต์ใน arr1 ที่มีค่าสูงกว่าอิลิเม้นต์ใน arr2 ณ เวลาในเซสชั่นเดียวกัน
    # เช่น ma1Arr = [102, 103, 104, 104]
    #   ma2Arr = [101, 102, 104, 105]
    #   totalHigherThan(ma1Arr, ma2Arr, 4)
    #   ผลลัพธ์คือ 2 -> 102 > 101, 103 > 102, 104 = 104, 104 < 105
    # =================================================================================================================
    @staticmethod
    def totalHigherThan(arr1, arr2, maxRange):
        total = 0
        for i in range(0, maxRange):
            if (arr1[i] > arr2[i]):
                total = total + 1
        return int(total)

    # =================================================================================================================
    # Return จำนวนแท่งเทียน/เซสชั่น ของอิลิเม้นต์ใน arr1 ที่มีค่าต่ำกว่าอิลิเม้นต์ใน arr2 ณ เวลาในเซสชั่นเดียวกัน
    # เช่น ma1Arr = [100, 102, 103, 104]
    #   ma2Arr = [101, 102, 104, 105]
    #   totalLowerThan(ma1Arr, ma2Arr, 4)
    #   ผลลัพธ์คือ 3 -> 100 < 101, 102 = 102, 103 < 104, 104 < 105
    # =================================================================================================================
    @staticmethod
    def totalLowerThan(arr1, arr2, maxRange):
        total = 0
        for i in range(0, maxRange):
            if (arr1[i] < arr2[i]):
                total = total + 1
        return int(total)

    # =================================================================================================================
    # Return level (ระดับ) ของ value ที่เทียบกับสเกลสูงสุดคือ maxLevel
    # เช่น levelEqually(78.2, 100, 10) = 8
    # =================================================================================================================
    @staticmethod
    def levelEqually(value, maxVal, maxLevel):
        return int(value / (maxVal / maxLevel)) + 1

    # =================================================================================================================
    # Return level (ระดับ) ของค่าในตำแหน่ง indexPosition ที่เทียบกับสเกลสูงสุดคือ maxLevel
    # เช่น
    # rsiArr = [18.9, 35.7, 38.2, 41.9]
    # levelEquallyAtIndex(rsiTDouble, 100, 10, 2) = 4 #ค่า 38.2 อยู่ใน level 4
    # =================================================================================================================
    @staticmethod
    def levelEquallyAtIndex(arr, maxVal, maxLevel, indexPosition):
        return int(round(arr[indexPosition] / (maxVal / maxLevel), 0)) + 1

    # =================================================================================================================
    # Return level (ระดับ) ของ MACD ปัจจุบัน
    # =================================================================================================================
    @staticmethod
    def macdLevelEqually(val, minVal, maxVal, maxLevel):
        level = 0

        if val < minVal:
            level = 1
        elif val > maxVal:
            level = maxLevel
        elif val == minVal:
            level = maxLevel - (maxLevel - 2)
        elif val == maxVal:
            level = maxLevel - 1
        elif val > minVal and val < maxVal:
            highest = maxVal + abs(minVal) - 0.00001
            level = int((abs(minVal - val) / (highest / (maxLevel - 2))) + 2)

        return level

    # =================================================================================================================
    # Return level (ระดับ) ของ MACD ในตำแหน่ง indexPosition ที่ต้องการ
    # =================================================================================================================
    @staticmethod
    def macdLevelEquallyAtIndex(arr, minVal, maxVal, maxLevel, indexPosition):
        level = FeatEngineer.macdLevelEqually(arr[indexPosition], minVal, maxVal, maxLevel)
        return int(level)

    # =================================================================================================================
    # Return 1 เมื่อพบไส้เทียนบนยาวมากกว่าหรือเท่ากับ shadowSize ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange
    # =================================================================================================================
    @staticmethod
    def foundLongUpperShadow(priceHighs, priceOpens, priceCloses, maxRange, shadowSize):
        found = 0
        for i in range(0, maxRange):
            lowerBound = max(priceOpens[i], priceCloses[i])
            if (priceHighs[i] - lowerBound >= shadowSize):
                found = 1
                break
        return int(found)

    # =================================================================================================================
    # Return 1 เมื่อพบไส้เทียนล่างยาวมากกว่าหรือเท่ากับ shadowSize ย้อนหลังแท่งเทียนหรือเซสชั่นกลับไปเท่ากับ maxRange
    # =================================================================================================================
    @staticmethod
    def foundLongLowerShadow(priceLows, priceOpens, priceCloses, maxRange, shadowSize):
        found = 0
        for i in range(0, maxRange):
            upperBound = min(priceOpens[i], priceCloses[i])
            if (upperBound - priceLows[i] >= shadowSize):
                found = 1
                break
        return int(found)

    # =================================================================================================================
    # Return ค่าระดับความห่าง -6 ถึง 6 ระหว่าง stochastic %K กับ %D โดยแบ่งเป็นช่วง ได้แก่
    # ระดับ 1 คือ diff <= -16, ระดับ 2 คือ -16 < diff <= -8, ระดับ 3 คือ -8 < diff <= -4, ระดับ 4 คือ -4 < diff <= -2,
    # ระดับ 5 คือ -2 < diff <= -1, ระดับ 6 คือ -1 < diff <= 0
    # ระดับ 7 คือ 0 > diff <= 1, ระดับ 8 คือ 1 < diff <= 2, ระดับ 9 คือ 2 < diff <= 4, ระดับ 10 คือ 4 < diff <= 8,
    # ระดับ 11 คือ 8 < diff <= 16 ระดับ 12 คือ diff > 16
    # parameters:
    # k - %K
    # d - %D
    # =================================================================================================================
    @staticmethod
    def stochdiff_scaling(k, d):
        result = 0
        max_scale = 12
        diff = k - d
        half_max_scale = int(max_scale / 2)

        if diff > 0:
            result = math.ceil(math.log(math.ceil(diff), 2) + 1)
            if result > half_max_scale:
                result = half_max_scale
            result = result + half_max_scale

        elif diff < 0:
            result = math.ceil(math.log(math.ceil(abs(diff)), 2) + 1)
            result = (half_max_scale - result) + 1
            if result <= 0:
                result = 1

        elif diff == 0:
            result = half_max_scale

        return result

    # =================================================================================================================
    # Return ค่าระดับความห่าง -6 ถึง 6 ระหว่าง rsi กับ ma ของ rsi โดยแบ่งเป็นช่วง ได้แก่
    # ระดับ 1 คือ diff <= -16, ระดับ 2 คือ -16 < diff <= -8, ระดับ 3 คือ -8 < diff <= -4, ระดับ 4 คือ -4 < diff <= -2,
    # ระดับ 5 คือ -2 < diff <= -1, ระดับ 6 คือ -1 < diff <= 0
    # ระดับ 7 คือ 0 > diff <= 1, ระดับ 8 คือ 1 < diff <= 2, ระดับ 9 คือ 2 < diff <= 4, ระดับ 10 คือ 4 < diff <= 8,
    # ระดับ 11 คือ 8 < diff <= 16 ระดับ 12 คือ diff > 16
    # parameters:
    # rsi - RSI
    # rsima - Moving Average ของ RSI
    # =================================================================================================================
    @staticmethod
    def rsidiff_scaling(rsi, rsima):
        result = 0
        max_scale = 12
        diff = float(rsi) - float(rsima)
        half_max_scale = int(max_scale / 2)

        if diff > 0:
            result = math.ceil(math.log(math.ceil(diff), 2) + 1)
            if result > half_max_scale:
                result = half_max_scale
            result = result + half_max_scale

        elif diff < 0:
            result = math.ceil(math.log(math.ceil(abs(diff)), 2) + 1)
            result = (half_max_scale - result) + 1
            if result <= 0:
                result = 1

        elif diff == 0:
            result = half_max_scale

        return result

    # =================================================================================================================
    # Return ค่าระดับความห่าง -8 ถึง 16 ระหว่าง price1 กับ price2 เช่น ใกล้กันมาก, ใกล้กัน, ห่างกัน, ห่างกันมาก, ห่างกันมากๆ
    # ประยุกต์ใช้ได้กับ ราคา, moving average, bollinger band (top, mid, bottom) เป็นต้น
    # โดยแบ่งเป็นช่วง ได้แก่
    # ระดับ 1 คือ diff <= -0.8, ระดับ 2 คือ -0.8 < diff <= -0.4, ระดับ 3 คือ -0.4 < diff <= -0.2,
    # ระดับ 4 คือ -0.2 < diff <= -0.1, ระดับ 5 คือ -0.1 < diff <= -0.05, ระดับ 6 คือ -0.05 < diff <= -0.025,
    # ระดับ 7 คือ -0.025 < diff <= -0.0125, ระดับ 8 คือ -0.0125 < diff <= 0
    # ระดับ 9 คือ 0 > diff <= 0.0125, ระดับ 10 คือ 0.0125 < diff <= 0.025, ระดับ 11 คือ 0.025 < diff <= 0.05,
    # ระดับ 12 คือ 0.05 < diff <= 0.1, ระดับ 13 คือ 0.1 < diff <= 0.2, ระดับ 14 คือ 0.2 < diff <= 0.4,
    # ระดับ 15 คือ 0.4 < diff <= 0.8, ระดับ 16 คือ diff >= 0.8
    # parameters:
    # price_open - open price
    # price_close - close price
    # =================================================================================================================
    @staticmethod
    def pricediff_scaling(price1, price2):
        result = 0
        max_scale = 16
        diff = round(float(price1) - float(price2), 4)
        half_max_scale = int(max_scale / 2)
        first_bin = 0.0125

        if diff > 0:
            result = math.ceil(math.log(diff / first_bin, 2) + 1)
            if result > half_max_scale:
                result = half_max_scale
            result = result + half_max_scale

        elif diff < 0:
            result = math.ceil(math.log(abs(diff / first_bin), 2) + 1)
            result = (half_max_scale - result) + 1
            if result <= 0:
                result = 1

        elif diff == 0:
            result = half_max_scale

        return result

    # =================================================================================================================
    # Return ค่าขนาดแท่งเทียน โดยขนาดเป็นค่า absolute แบ่งขนาดแท่งเทียนเป็น 6 ขนาด ได้แก่
    # ขนาดที่ 1 คือ มีขนาดเล็กกว่า 0.0125
    # ขนาดที่ 2 คือ 0.0125 > size <= 0.025
    # ขนาดที่ 3 คือ 0.025 > size <= 0.0.05
    # ขนาดที่ 4 คือ 0.05 > size <= 0.1
    # ขนาดที่ 5 คือ 0.1 > size <= 0.2
    # ขนาดที่ 6 คือ มีขนาดใหญ่กว่า 0.2 ขึ้นไป
    # parameters:
    # price_open - ราคาเปิด ที่ผ่านการทำ feature scaling ให้ค่าอยู่ในช่วง 0 - 1 มาแล้ว
    # price_close - ราคาปิด ที่ผ่านการทำ feature scaling ให้ค่าอยู่ในช่วง 0 - 1 มาแล้ว
    # =================================================================================================================
    @staticmethod
    def cdssize_scaling(price_open, price_close):
        result = 0
        diff = float(price_open) - float(price_close)
        diff = round(abs(diff), 4)

        if diff <= 0.0125:
            result = 1
        elif diff > 0.0125 and diff <= 0.025:
            result = 2
        elif diff > 0.025 and diff <= 0.05:
            result = 3
        elif diff > 0.05 and diff <= 0.1:
            result = 4
        elif diff > 0.1 and diff <= 0.2:
            result = 5
        elif diff > 0.2:
            result = 6

        return result

# END OF CLASS DEFINITION
