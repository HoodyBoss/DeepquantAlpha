"""
A class for handling the signal processing

Algorithm:
1. เซ็ตค่า bar เริ่มต้นโดยกำหนด datetime เพื่อโหลด trade states จาก bar นั้นถึง bar ล่าสุด
2. โหลด trade states จาก database แล้วเก็บลง pandas
3. รันเมธอด process_trade_details()
4. รันเมธอด prepare trade signal() แล้วเซ็ตค่า signal details ต่างๆ ลง trade states row ล่าสุด
5. append trade states row ล่าสุดลง trade states (pandas)
6. insert trade states row ล่าสุดลง database
"""

class SignalProcessor():


    # ========================================================================================================================
    # BEGIN: Signal processing
    # ========================================================================================================================
    def process_trade_details(self):

        startBar = 0
        endBar = 0
        if hasRunPreviousBars != 1:
            startBar = 1
            if tradeStart == 1:
                endBar = BarCount - 1
            else:
                endBar = BarCount
        elif hasRunPreviousBars == 1:
            startBar = BarCount - 1
            if tradeStart == 1:
                endBar = BarCount
        
        for i in range(startBar, endBar):
            if i == 1:
                modelSignalCode = 3 # default signal
                modelStopLoss = 0.0
                modelEntryPosSizePercent = 0.0
                modelScaleSize = 0.0

                prevTradeState = 0
                tradeState = 0
                firstBarOfTrade = 0
                barsSinceEntry = 0
                priceAtEntry = 0.0
                cumProfit = 0.0
                cumLoss = 0.0
                cumWin = 0.0
                cumLose = 0.0
                totalTrade = 0

                highestHighSinceEntry = 0.0
                lowestLowSinceEntry = 0.0
                highestCloseSinceEntry = 0.0
                lowestCloseSinceEntry = 0.0

                profit = 0.0
                maxProfitSinceEntry = 0.0

                curAmiTrade = 0 # 1 = long, 2 = short
                isStopOutBeforeSellOrCover = 0

                scale = 0 # 1 = scale in, 2 = scale out
                scaleSize = 0.0
                scaleInCount = 0
                scaleOutCount = 0

                scaledIn1_count = 0
                isScaledIn1 = 0
                isScaledIn2 = 0
                isScaledIn3 = 0

                # type = 0 = stopTypeLoss - maximum loss stop, 1 = stopTypeProfit - profit target stop,
                # 2 = stopTypeTrailing - trailing stop, 3 = stopTypeNBar - N-bar stop.mode = 0 - disable
                stop = -1
                stopLong = 0
                stopShort = 0

                stopLoss = 0

            # Uses for debug
            if i == 4800:
                dt = DateTimeToStr( DateNumberArray[i])
                print(NumToStr(i))

            # tradeState value: 0 = none(has no position), 1 = long, 2 = short
            if Buy[i - 1] == 1:
                if (curAmiTrade[i - 1] == 1 and isStopOutBeforeSellOrCover[i - 1] == 0)\
                    or curAmiTrade[i - 1] == 2\
                    or curAmiTrade[i - 1] == 0:
                    tradeState[i] = 1
                curAmiTrade[i] = 1

            if isStopOutBeforeSellOrCover[i - 1] == 1 and curAmiTrade[i - 1] == 1\
                and changeTrailStopToScaleOut == 1:
                Buy[i] = 0
                Cover[i] = 0
                tradeState[i] = 0
                curAmiTrade[i] = 0
            elif Short[i - 1] == 1:
                if (curAmiTrade[i - 1] == 2 and isStopOutBeforeSellOrCover[i - 1] == 0)\
                    or curAmiTrade[i - 1] == 1 or curAmiTrade[i - 1] == 0:
                    tradeState[i] = 2
                    curAmiTrade[i] = 2

            if isStopOutBeforeSellOrCover[i - 1] == 1 and curAmiTrade[i - 1] == 2\
                and changeTrailStopToScaleOut == 1:
                Short[i] = 0
                Sell[i] = 0
                tradeState[i] = 0
                curAmiTrade[i] = 0
            elif Sell[i - 1] == 1:
                tradeState[i] = 0
                curAmiTrade[i] = 0
            elif Cover[i - 1] == 1:
                tradeState[i] = 0
                curAmiTrade[i] = 0
            else:
                if i > 0:
                    tradeState[i] = tradeState[i - 1]

            updatePrevTradeState(i)

            if enableReEntryTrade == 1:
                if curAmiTrade[i - 1] == 1 and isStopOutBeforeSellOrCover[i - 1] == 1\
                    and (stop[i - 1] == 0 or stop[i - 1] == 2):
                    Sell[i] = 1
                    tradeState[i] = 0
                    isStopOutBeforeSellOrCover[i] = 0
                    curAmiTrade[i] = 0
                elif curAmiTrade[i - 1] == 2 and isStopOutBeforeSellOrCover[i - 1] == 1\
                    and (stop[i - 1] == 0 or stop[i - 1] == 2):
                    Cover[i] = 1
                    tradeState[i] = 0
                    isStopOutBeforeSellOrCover[i] = 0
                    curAmiTrade[i] = 0
            elif enableReEntryTrade == 0:
                if curAmiTrade[i - 1] == 0 and isStopOutBeforeSellOrCover[i - 1] == 1:
                    if (prevTradeState[i] == 1 and curAmiTrade[i] == 1 and tradeState[i] == 1)\
                        or (prevTradeState[i] == 2 and curAmiTrade[i] == 2 and tradeState[i] == 2):
                        curAmiTrade[i] = 0
                        tradeState[i] = 0
                        isStopOutBeforeSellOrCover[i] == 1

            updatePrevTradeState(i)

            if tradeState[i] == tradeState[i - 1]:
                curAmiTrade[i] = curAmiTrade[i - 1]
                isStopOutBeforeSellOrCover[i] = isStopOutBeforeSellOrCover[i - 1]

            if i > 1 and tradeState[i] != tradeState[i - 1]:
                if tradeState[i] == 1 or tradeState[i] == 2:
                    firstBarOfTrade[i] = i
                    priceAtEntry[i] = Close[i - 1]
                    barsSinceEntry[i] = 1
                    highestHighSinceEntry[i] = High[i]
                    lowestLowSinceEntry[i] = Low[i]
                    highestCloseSinceEntry[i] = Close[i]
                    lowestCloseSinceEntry[i] = Close[i]
                    totalTrade[i] = totalTrade[i - 1] + 1

                    switch tradeState[i]:
                    case 1:
                        profit[i] = Close[i] - priceAtEntry[i]
                        maxProfitSinceEntry[i] = profit[i]
                        break
                    case 2:
                        profit[i] = priceAtEntry[i] - Close[i]
                        maxProfitSinceEntry[i] = profit[i]
                        break
                    default:
                        firstBarOfTrade[i] = 0.0
                        priceAtEntry[i] = 0.0
                        barsSinceEntry[i] = 0.0
                        highestHighSinceEntry[i] = 0.0
                        lowestLowSinceEntry[i] = 0.0
                        highestCloseSinceEntry[i] = 0.0
                        lowestCloseSinceEntry[i] = 0.0
                        totalTrade[i] = totalTrade[i - 1]

                    if tradeState[i - 1] != 0:
                        if profit[i - 1] >= 0:
                            cumWin[i] = cumWin[i - 1] + 1
                            cumLose[i] = cumLose[i - 1]

                            cumProfit[i] = cumProfit[i - 1] + profit[i - 1]
                            cumLoss[i] = cumLoss[i - 1]
                        else:
                            cumLose[i] = cumLose[i - 1] + 1
                            cumWin[i] = cumWin[i - 1]

                            cumLoss[i] = cumLoss[i - 1] + profit[i - 1]
                            cumProfit[i] = cumProfit[i - 1]
                    else:
                        cumWin[i] = cumWin[i - 1]
                        cumLose[i] = cumLose[i - 1]

                        cumProfit[i] = cumProfit[i - 1]
                        cumLoss[i] = cumLoss[i - 1]

            elif i > 1 and tradeState[i] == tradeState[i - 1]:
                if tradeState[i] == 1 or tradeState[i] == 2:
                    firstBarOfTrade[i] = firstBarOfTrade[i - 1]
                    priceAtEntry[i] = priceAtEntry[i - 1]
                    barsSinceEntry[i] = barsSinceEntry[i - 1] + 1
                    highestHighSinceEntry[i] = maxVal(High[i], highestHighSinceEntry[i - 1])
                    lowestLowSinceEntry[i] = minVal(Low[i], lowestLowSinceEntry[i - 1])
                    highestCloseSinceEntry[i] = maxVal(Close[i], highestCloseSinceEntry[i - 1])
                    lowestCloseSinceEntry[i] = minVal(Close[i], lowestCloseSinceEntry[i - 1])

                    switch tradeState[i]:
                    case 1:
                        profit[i] = Close[i] - priceAtEntry[i]
                        maxProfitSinceEntry[i] = maxVal(maxProfitSinceEntry[i - 1], High[i] - priceAtEntry[i])
                        break
                    case 2:
                        profit[i] = priceAtEntry[i] - Close[i]
                        maxProfitSinceEntry[i] = maxVal(maxProfitSinceEntry[i - 1], priceAtEntry[i] - Low[i])
                        break
                    default:
                        firstBarOfTrade[i] = 0.0
                        priceAtEntry[i] = 0.0
                        barsSinceEntry[i] = 0.0
                        highestHighSinceEntry[i] = 0.0
                        lowestLowSinceEntry[i] = 0.0
                        highestCloseSinceEntry[i] = 0.0
                        lowestCloseSinceEntry[i] = 0.0

                        totalTrade[i] = totalTrade[i - 1]
                        cumWin[i] = cumWin[i - 1]
                        cumLose[i] = cumLose[i - 1]
                        cumProfit[i] = cumProfit[i - 1]
                        cumLoss[i] = cumLoss[i - 1]

            # BEGIN: Handle stop
            triggeredMaxStop = predictMaxStop(i)
            triggeredTrailStop = 0
            if isLastBar[i] == 0:
                triggeredTrailStop = predictTrailStop(i)

            if triggeredMaxStop == 1 and stop[i-1] != 0 and stop[i-1] != 2\
                and not(tradeState[i] != tradeState[i - 1])\
                and not(tradeState[i] == 1 and (Sell[i] == 1 or Short[i] == 1))\
                and not(tradeState[i] == 2 and (Buy[i] == 1 or Cover[i] == 1)):
                stop[i] = 0
            elif triggeredTrailStop == 1\
                and stop[i-1] != 0 and stop[i-1] != 2\
                and not(tradeState[i] != tradeState[i - 1])\
                and not(tradeState[i] == 1 and (Sell[i] == 1 or Short[i] == 1))\
                and not(tradeState[i] == 2 and (Buy[i] == 1 or Cover[i] == 1)):
                stop[i] = 2

                if tradeState[i-1] == 1:
                    stopLong[i] = 1
                elif tradeState[i - 1] == 2:
                    stopShort[i] = 1
                elif stop[i - 1] == 0 or stop[i-1] == 2:
                    stop[i] = -1
                    stopLong[i] = 0
                    stopShort[i] = 0
                    tradeState[i] = 0.0
                    priceAtEntry[i] = 0.0
                    barsSinceEntry[i] = 0
                    highestHighSinceEntry[i] = 0.0
                    lowestLowSinceEntry[i] = 0.0
                    highestCloseSinceEntry[i] = 0.0
                    lowestCloseSinceEntry[i] = 0.0
                    profit[i] = 0.0
                    maxProfitSinceEntry[i] = 0.0

            updatePrevTradeState(i)

            if (stop[i] == 0 or stop[i] == 2) and changeTrailStopToScaleOut == 0:
                if tradeState[i - 1] == 1:
                    SellSignal[i] = 1
                elif tradeState[i - 1] == 2:
                    CoverSignal[i] = 1

                    isStopOutBeforeSellOrCover[i] = 1
            elif stop[i] == 0 and changeTrailStopToScaleOut == 1:
                isStopOutBeforeSellOrCover[i] = 1
            elif stop[i - 1] == 0 or stop[i - 1] == 2:
                isStopOutBeforeSellOrCover[i] = 1
            # END: Handle stop

            # BEGIN: Handle scale out
            scaleOutSize = 0
            if isLastBar[i] == 0\
                and ((tradeState[i] == 1 and Short[i] == 0)\
                     or (tradeState[i] == 2 and Buy[i] == 0))\
                and scaleOutCount[i - 1] < scaleOutMax[i]:
                scaleOutSize = predictScaleOutSize(i)

            if changeTrailStopToScaleOut == 1 and stop[i] == 2 and scaleOutCount[i-1] < scaleOutMax[i] + 1:
                # scale[i] = 2
                scaleOutSize = changeTrailStopToScaleOutSize
                # scaleOutCount[i] = scaleOutCount[i-1] + 1

            if stop[i] != 0 and (stop[i] != 2 or (changeTrailStopToScaleOut == 1 and stop[i] == 2))\
                and i > 1 and tradeState[i] == tradeState[i - 1]\
                and (scaleOutCount[i-1] < scaleOutMax[i]\
                     or (changeTrailStopToScaleOut == 1 and scaleOutCount[i-1] < scaleOutMax[i] + 1)):
                if tradeState[i] == 1:
                    if scaleOutSize > 0:
                        scale[i] = 2
                        scaleSize[i] = scaleOutSize
                        scaleOutCount[i] = scaleOutCount[i-1] + 1
                elif tradeState[i] == 2:
                    if scaleOutSize > 0:
                        scale[i] = 2
                        scaleSize[i] = scaleOutSize
                        scaleOutCount[i] = scaleOutCount[i-1] + 1

            if scaleOutSize == 0:
                scaleOutCount[i] = scaleOutCount[i - 1]

            if ((scale[i - 1] == 2 and scaleSize[i-1] > 0) or scaleOutCount[i-1] > 0)\
                and tradeState[i] == tradeState[i-1] and scaleOutCount[i-1] == scaleOutMax[i]\
                and changeTrailStopToScaleOut == 0:
                scale[i] = 0
                scaleSize[i] = 0
                scaleOutCount[i] = scaleOutCount[i-1]
            elif ((scale[i-1] == 2 and scaleSize[i-1] > 0) or scaleOutCount[i-1] > 0)\
                and tradeState[i] == tradeState[i-1] and scaleOutCount[i-1] == scaleOutMax[i] + 1\
                and changeTrailStopToScaleOut == 1:
                scale[i] = 0
                scaleSize[i] = 0
                scaleOutCount[i] = scaleOutCount[i-1]

            if tradeState[i] != tradeState[i-1]:
                scaleOutCount[i] = 0

            if changeTrailStopToScaleOut == 1 and stop[i] == 2:
                stop[i] = -1
                stopLong[i] = 0
                stopShort[i] = 0

            """
            if scaleOutCount[i] > scaleOutMax:
                scale[i] = 0
                scaleSize[i] = 0
                scaleOutCount[i] = scaleOutCount[i-1]
            """
            # END: Handle scale out

            # BEGIN: Handle scale in
            if (tradeState[i] == 1 and tradeState[i - 1] != 1)\
                or (tradeState[i] == 2 and tradeState[i - 1] != 2):
                # Reset
                scale in state after entered new trade
                scaledIn1_count[i] = 0
                isScaledIn1[i] = 0
                isScaledIn2[i] = 0
                isScaledIn3[i] = 0

            if isLastBar[i] == 0 and barsSinceEntry[i] > 1\
                and stop[i] != 0 and stop[i] != 2\
                and scale[i] == 0\
                and enableScaleIn1 == 1 and isScaledIn1[i-1] == 0\
                and scaledIn1_count[i-1] < scaledIn1_max\
                and isScaledIn2[i-1] == 0 and isScaledIn3[i-1] == 0:
                scaleSize[i] = predictScaleIn1_Size(i, scaledIn1_count[i-1], scaledIn1_max)

            if scaleSize[i] > 0 and scaledIn1_count[i-1] < scaledIn1_max:
                scale[i] = 1
                scaledIn1_count[i] = scaledIn1_count[i - 1] + 1
                scaleInCount[i] = scaleInCount[i - 1] + 1

            if scaledIn1_count[i] == scaledIn1_max:
                isScaledIn1[i] = 1

            if isLastBar[i] == 0 and stop[i] != 0 and stop[i] != 2\
                and (tradeState[i] == 1 or tradeState[i] == 2)\
                and scale[i] == 0\
                and enableScaleIn2 == 1\
                and isScaledIn2[i-1] == 0 and isScaledIn3[i-1] == 0 and isScaledIn1[i-1] == 1:

                scaleSize[i] = predictScaleIn2_Size(i)

            if scaleSize[i] > 0:
                scale[i] = 1
                isScaledIn2[i] = 1
                scaleInCount[i] = scaleInCount[i - 1] + 1

                isScaledIn1[i] = isScaledIn1[i - 1]
                scaledIn1_count[i] = scaledIn1_count[i - 1]

            if isLastBar[i] == 0 and stop[i] != 0 and stop[i] != 2\
                and scale[i] == 0\
                and enableScaleIn3 == 1\
                and isScaledIn3[i-1] == 0 and isScaledIn1[i-1] == 1 and isScaledIn2[i-1] == 1:

                scaleSize[i] = predictScaleIn3_Size(i)

            if scaleSize[i] > 0:
                scale[i] = 1
                isScaledIn3[i] = 1
                scaleInCount[i] = scaleInCount[i - 1] + 1

                isScaledIn1[i] = isScaledIn1[i - 1]
                isScaledIn2[i] = isScaledIn2[i - 1]
                scaledIn1_count[i] = scaledIn1_count[i - 1]

            if scale[i] != 1 and tradeState[i] == tradeState[i-1]:
                scaleInCount[i] = scaleInCount[i-1]

                scaledIn1_count[i] = scaledIn1_count[i-1]
                isScaledIn1[i] = isScaledIn1[i-1]
                isScaledIn2[i] = isScaledIn2[i-1]
                isScaledIn3[i] = isScaledIn3[i-1]

            if hasRunPreviousBars == 1:
                if scaleInCount[i] == scaleInCount[i-1] and scale[i] == 1 and scaleSize[i] > 0:
                    scale[i] = 0
                    scaleSize[i] = 0.0

            if tradeState[i] != tradeState[i - 1]:
                scaleInCount[i] = 0
            # END: Handle scale in

            # BEGIN: Validate scale in / out
            if scaleOutCount[i - 1] == scaleOutMax + 1 and scale[i] == 2:
                scale[i] = 0
            elif scaledIn1_count[i - 1] == scaledIn1_max and scale[i] == 1\
                and isScaledIn2[i] != 1 and isScaledIn3[i] != 1:
                scale[i] = 0

            if scaleInCount[i] == scaleInCount[i-1] and scaleOutCount[i] == scaleOutCount[i-1]:
                scale[i] = 0
                scaleSize[i] = 0.0
            # END: Validate scale in / out

            # Predict entry's position size in percentage
            # Amibroker's signal code, 1 = Buy, 2 = Sell, 3 = Short, 4 = Cover
            if Buy[i] == 1 and tradeState[i] != 1:
                entryPosSizePercent[i] = predictEntryPosSizePercent(1, i)
            elif Short[i] == 1 and tradeState[i] != 2:
                entryPosSizePercent[i] = predictEntryPosSizePercent(2, i)
            elif tradeState[i] != tradeState[i - 1] and tradeState[i] != 0:
                entryPosSizePercent[i] = entryPosSizePercent[i - 1]
            elif tradeState[i] == tradeState[i - 1] and tradeState[i] != 0:
                entryPosSizePercent[i] = entryPosSizePercent[i - 1]
            elif tradeState[i] == 0:
                entryPosSizePercent[i] = 0

            # Summarize entry signal to be used to predict stop loss
            entrySignal = 0
            if Buy[i] == 1 and (enableReEntryTrade == 1 or (enableReEntryTrade == 0 and prevTradeState[i] != 1)):
                if (curAmiTrade[i] != 1 and isStopOutBeforeSellOrCover[i] == 0)\
                    or curAmiTrade[i] == 2\
                    or curAmiTrade[i] == 0:
                    entrySignal = 1

            if isStopOutBeforeSellOrCover[i] == 1 and curAmiTrade[i] == 2\
                and changeTrailStopToScaleOut == 1:
                entrySignal = 1
            elif Short[i] == 1 and (enableReEntryTrade == 1 or (enableReEntryTrade == 0 and prevTradeState[i] != 2)):
                if (curAmiTrade[i] != 2 and isStopOutBeforeSellOrCover[i] == 0)\
                    or curAmiTrade[i] == 1\
                    or curAmiTrade[i] == 0:
                    entrySignal = 2

            if isStopOutBeforeSellOrCover[i] == 1 and curAmiTrade[i] == 1\
                and changeTrailStopToScaleOut == 1:
                entrySignal = 2

            # Define stop loss position(unit is point not price or index)
            if entrySignal == 1 or entrySignal == 2:
                stopLoss[i] = predictStopLoss(i, entrySignal)
            elif tradeState[i] == 1 or tradeState[i] == 2:
                stopLoss[i] = stopLoss[i - 1]
# ========================================================================================================================
# END: Signal processing
# ========================================================================================================================
        