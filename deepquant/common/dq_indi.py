import pandas as pd
import talib

import warnings

warnings.simplefilter(action="ignore", category=RuntimeWarning)

from deepquant.common.felib import FELib


class DQIndi():
    """
    หลาย indicator ต้องใช้ค่า digits_num ซึ่งหมายถึง จำนวนที่จะคูณราคาแล้วทำให้ราคาระดับ pip เป็นเลขจำนวนเต็ม
    เพื่อช่วยให้ง่ายในการนำไปคำนวณอื่นๆ ต่อไป
    เช่น
        1) GOLD มีราคา 1482.25 และ digits_num มีค่า 10.0
            ราคา GOLD จะเป็น 1482.25 * digits_num = 14822.5
        2) EURUSD มีราคา 1.12345 และ digits_num มีค่า 10000.0
            ราคา EURUSD จะเป็น 1.12345 * digits_num = 11234.5

    ค่า digits_num ก็คือค่า 10 ยกกำลัง จำนวนหลักทศนิยมสูงสุดของสินค้า ลบด้วย 1
    เช่น
        1) GOLD ทั่วไปมีทศนิยม 2 ตำแหน่ง ดังนั้น digits_num มีค่าเท่ากับ 10 ยกกำลัง (2-1) = 10
        2) EURUSD ทั่วไปมีทศนิยม 5 ตำแหน่ง ดังนั้น digits_num มีค่าเท่ากับ 10 ยกกำลัง (5-1) = 10
    """

    # ==============================================================================================================================
    @staticmethod
    def price_channel_code(digits_num, p_high, p_low, period, channel1_val, channel2_val):
        # INDI: Price Channel Code
        HighestHigh = FELib.hhv(p_high, period) * digits_num
        LowestLow = FELib.llv(p_low, period) * digits_num
        HighLowRange = HighestHigh - LowestLow

        channel1 = pd.Series([channel1_val] * len(HighLowRange))
        channel2 = pd.Series([channel2_val] * len(HighLowRange))

        channelColorCode = FELib.iif(HighLowRange <= channel1 \
                                     , 1 \
                                     , FELib.iif((HighLowRange > channel1) & (HighLowRange <= channel2), 2, 3))

        channelColorCode = pd.Series(channelColorCode)
        return channelColorCode

    # ==============================================================================================================================
    @staticmethod
    def sideway_code(digits_num, channel_code, p_close, channel_range1, channel_range2, channel_range3 \
                    , ma, stddev_ma_period, ma_stddev_const, ma_hl_period, ma_close_period1, ma_close_period2):
        # INDI: Sideway Code
        diffLevel = FELib.iif(channel_code == 1, channel_range1 \
                              , FELib.iif(channel_code == 2, channel_range2, channel_range3))

        ma3Stdev = talib.STDDEV(ma, stddev_ma_period) * digits_num

        maStdevLevel = FELib.iif(channel_code == 1, ma_stddev_const \
                                 , FELib.iif(channel_code == 2, ma_stddev_const * 2, ma_stddev_const * 3))

        sidewayCode = FELib.iif((((ma - FELib.llv(ma, ma_hl_period)) * digits_num < diffLevel / 2.5) \
                                | ((FELib.hhv(ma, ma_hl_period) - ma) * digits_num < diffLevel / 2.5)) \
                                & (ma3Stdev < maStdevLevel) \
                                & (abs(ma - talib.SMA(p_close, ma_close_period1)) * digits_num < diffLevel), 2, 1)

        sidewayCode = FELib.iif(abs(talib.EMA(p_close, ma_close_period2) - ma) * digits_num < diffLevel / 2, 2, sidewayCode)
        return sidewayCode

    # ==============================================================================================================================
    @staticmethod
    def trend_code(channel_code, p_close, macd1_short_period, macd1_long_period, macd1_signal_period \
                    , macd2_short_period, macd2_long_period, macd2_signal_period \
                    , macd3_short_period, macd3_long_period, macd3_signal_period \
                    , rolling_period1, rolling_period2, rolling_period3, ma):
        # INDI: Trend Code
        MACDLarge1, MACDSignalLarge1, MACDHist1 = talib.MACD(p_close, macd1_short_period, macd1_long_period, macd1_signal_period)
        if macd2_short_period > 0:
            MACDLarge2, MACDSignalLarge2, MACDHist2 = talib.MACD(p_close, macd2_short_period, macd2_long_period, macd2_signal_period)
        if macd3_short_period > 0:
            MACDLarge3, MACDSignalLarge3, MACDHist3 = talib.MACD(p_close, macd3_short_period, macd3_long_period, macd3_signal_period)

        if macd2_short_period > 0 and macd3_short_period > 0:
            MACDLarge = FELib.iif(channel_code == 1, MACDLarge1, FELib.iif(channel_code == 2, MACDLarge2, MACDLarge3))
            MACDSignalLarge = FELib.iif(channel_code == 1, MACDSignalLarge1 \
                                        , FELib.iif(channel_code == 2, MACDSignalLarge2, MACDSignalLarge3))
        else:
            MACDLarge = MACDLarge1
            MACDSignalLarge = MACDSignalLarge1

        rolling = FELib.iif(channel_code == 1, rolling_period1 \
                            , FELib.iif(channel_code == 2, rolling_period2, rolling_period3))

        trendCode = FELib.iif((ma <= talib.SMA(ma, rolling[0])) & (MACDLarge < MACDSignalLarge) \
                              , 2, FELib.iif((ma <= talib.SMA(ma, rolling[0])) & (MACDLarge >= MACDSignalLarge) \
                                             , 1, FELib.iif((ma >= talib.SMA(ma, rolling[0])) & (MACDLarge > MACDSignalLarge), 4, 3)))
        return trendCode

    # ==============================================================================================================================
    @staticmethod
    def rsi_macd_code(channel_code, p_close, rsi_period, ma_rsi_period \
                    , macd1_short_period, macd1_long_period, macd1_signal_period \
                    , macd2_short_period, macd2_long_period, macd2_signal_period \
                    , macd3_short_period, macd3_long_period, macd3_signal_period):
        # INDI: RSI - MACD Code
        MA_RSI = talib.SMA(talib.RSI(p_close, rsi_period), ma_rsi_period)
        rsiCode = FELib.iif(MA_RSI <= 50 \
                            , FELib.iif(MA_RSI >= talib.SMA(MA_RSI, 10), 1, 2) \
                            , FELib.iif(MA_RSI <= talib.SMA(MA_RSI, 10), 3, 4))

        MACDLarge1, MACDSignalLarge1, MACDHist1 = talib.MACD(p_close, macd1_short_period, macd1_long_period, macd1_signal_period)
        MACDLarge2, MACDSignalLarge2, MACDHist2 = talib.MACD(p_close, macd2_short_period, macd2_long_period, macd2_signal_period)
        MACDLarge3, MACDSignalLarge3, MACDHist3 = talib.MACD(p_close, macd3_short_period, macd3_long_period, macd3_signal_period)

        MACDLarge = FELib.iif(channel_code == 1, MACDLarge1, FELib.iif(channel_code == 2, MACDLarge2, MACDLarge3))
        MACDSignalLarge = FELib.iif(channel_code == 1, MACDSignalLarge1 \
                                        , FELib.iif(channel_code == 2, MACDSignalLarge2, MACDSignalLarge3))

        macdCode = FELib.iif(MACDLarge <= MACDSignalLarge \
                             , FELib.iif(MACDSignalLarge >= 0, 1, 2) \
                             , FELib.iif(MACDSignalLarge < 0, 3, 4))

        rsiMacdCode = FELib.round_num((rsiCode + macdCode) / 2, 0)
        return rsiMacdCode

    # ==============================================================================================================================
    @staticmethod
    def macd_code(p_close, short_period, long_period, signal_period, muliplier):
        # INDI: MACD Code
        MACDSuperLarge, MACDSignalSuperLarge, MACDHistSuperLarge = talib.MACD(p_close \
                                        , short_period, long_period, signal_period)
        MACDSuperLarge = MACDSuperLarge * muliplier
        MACDSignalSuperLarge = MACDSignalSuperLarge * muliplier

        macdSuperLargeCode = FELib.iif(MACDSuperLarge <= MACDSignalSuperLarge \
                                       , FELib.iif(MACDSignalSuperLarge >= 0, 1, 2) \
                                       , FELib.iif(MACDSignalSuperLarge < 0, 3, 4))
        return macdSuperLargeCode

    # ==============================================================================================================================
    @staticmethod
    def atr_code(p_high, p_low, p_close, period, ma_period1, ma_period2, ma_period3, ma_period4 \
            , hl_period, multiplier):
        # INDI: MA - ATR Code
        MA_ATR_Original = talib.EMA(talib.EMA(talib.ATR(p_high, p_low, p_close, period), ma_period1) \
                                    , ma_period2) * multiplier
        MA_ATR = talib.EMA(MA_ATR_Original, ma_period3)

        atrCode = FELib.iif(MA_ATR <= talib.SMA(MA_ATR, ma_period4) \
                            , FELib.iif(MA_ATR != FELib.llv(MA_ATR, hl_period ), 1, 2) \
                            , FELib.iif(MA_ATR != FELib.hhv(MA_ATR, hl_period ), 3, 4))
        return atrCode

    # ==============================================================================================================================
    @staticmethod
    def highlow_zone_code(channel_code, p_close, macd1_short_period, macd1_long_period, macd1_signal_period \
                            , macd2_short_period, macd2_long_period, macd2_signal_period \
                            , macd3_short_period, macd3_long_period, macd3_signal_period \
                            , large_stochd, ma_large_stochd, highlow_upper_zone, highlow_lower_zone):
        # INDI: High / Low Zone Code
        MACDLarge1, MACDSignalLarge1, MACDHist1 = talib.MACD(p_close, macd1_short_period, macd1_long_period, macd1_signal_period)
        if macd2_short_period > 0:
            MACDLarge2, MACDSignalLarge2, MACDHist2 = talib.MACD(p_close, macd2_short_period, macd2_long_period, macd2_signal_period)
        if macd3_short_period > 0:
            MACDLarge3, MACDSignalLarge3, MACDHist3 = talib.MACD(p_close, macd3_short_period, macd3_long_period, macd3_signal_period)

        if macd2_short_period > 0 and macd3_short_period > 0:
            MACDLarge = FELib.iif(channel_code == 1, MACDLarge1, FELib.iif(channel_code == 2, MACDLarge2, MACDLarge3))
            MACDSignalLarge = FELib.iif(channel_code == 1, MACDSignalLarge1 \
                                        , FELib.iif(channel_code == 2, MACDSignalLarge2, MACDSignalLarge3))
        else:
            MACDLarge = MACDLarge1
            MACDSignalLarge = MACDSignalLarge1

        MACDCode = FELib.iif(MACDLarge <= MACDSignalLarge \
                             , FELib.iif(MACDSignalLarge >= 0, 1, 2) \
                             , FELib.iif(MACDSignalLarge < 0, 3, 4))

        MA_Cur_Large_StochD = talib.SMA(large_stochd, ma_large_stochd)

        HighLowZoneCode = FELib.iif((MA_Cur_Large_StochD < highlow_lower_zone) & (MACDCode == 2) \
                                    , 1, FELib.iif((MA_Cur_Large_StochD > highlow_upper_zone) & ((MACDCode == 3) | (MACDCode == 4)), 3, 2))
        return HighLowZoneCode

    # ==============================================================================================================================
    @staticmethod
    def refine_trend_code(p_close, trend_code, channel_code, sideway_code \
                            , ma_short, ma_long, period1, period2, period3):
        # INDI: Refined Trend Code
        trend_code = FELib.iif((trend_code == 4) & (talib.SMA(p_close, period2) < talib.SMA(ma_short, period2)), 3, trend_code)
        trend_code = FELib.iif((trend_code == 4) & (talib.SMA(p_close, period2) < talib.SMA(ma_long, period2)) \
                              & (talib.SMA(p_close, period3) < talib.SMA(ma_short, period3)), 3, trend_code)
        trend_code = FELib.iif((trend_code == 4) & (talib.SMA(p_close, period1) < talib.SMA(ma_long, period1)), 3, trend_code)
        trend_code = FELib.iif((trend_code == 2) & (talib.SMA(p_close, period2) > talib.SMA(ma_short, period2)), 1, trend_code)
        trend_code = FELib.iif((trend_code == 2) & (talib.SMA(p_close, period2) > talib.SMA(ma_long, period2)) \
                              & (talib.SMA(p_close, period3) > talib.SMA(ma_short, period3)), 1, trend_code)
        trend_code = FELib.iif((trend_code == 2) & (talib.SMA(p_close, period1) > talib.SMA(ma_long, period1)), 1, trend_code)

        trend_code = FELib.iif((channel_code == 1) & (sideway_code == 2) & (ma_short < ma_long) & (trend_code == 4), 3, trend_code)
        trend_code = FELib.iif((channel_code == 1) & (sideway_code == 2) & (ma_short > ma_long) & (trend_code == 2), 1, trend_code)
        return trend_code

    # ==============================================================================================================================
    @staticmethod
    def macd_momentum_code(macd_signal, upper_level1, upper_level2, lower_level1, lower_level2):
        MACDMomentumCode = FELib.iif((macd_signal >= lower_level1) & (macd_signal <= upper_level1), 1, 0)
        MACDMomentumCode = FELib.iif((MACDMomentumCode == 0)
                                     & (((macd_signal > upper_level1) & (macd_signal <= upper_level2))
                                        | ((macd_signal >= lower_level2) & (macd_signal < lower_level1)))
                                     , 2, MACDMomentumCode)
        MACDMomentumCode = FELib.iif((MACDMomentumCode == 0) & ((macd_signal > upper_level2) | (macd_signal < lower_level2))
                                     , 3, MACDMomentumCode)
        return MACDMomentumCode * 1.0

    # ==============================================================================================================================
    @staticmethod
    def ma_volatility_code(ma_short, ma_long, range1, range2, range3):
        MAVolatilityCode = FELib.iif((ma_short > ma_long) & (ma_short - ma_long < range1), 5, 0)
        MAVolatilityCode = FELib.iif((MAVolatilityCode == 0) & (ma_short > ma_long) & (ma_short - ma_long >= range1) & (ma_short - ma_long < range2)
                                     , 6, MAVolatilityCode)
        MAVolatilityCode = FELib.iif((MAVolatilityCode == 0) & (ma_short > ma_long) & (ma_short - ma_long >= range2) & (ma_short - ma_long < range3)
                                     , 7, MAVolatilityCode)
        MAVolatilityCode = FELib.iif((MAVolatilityCode == 0) & (ma_short > ma_long) & (ma_short - ma_long >= range3), 8, MAVolatilityCode)

        MAVolatilityCode = FELib.iif((MAVolatilityCode == 0) & (ma_short < ma_long) & (ma_long - ma_short < range1), 1, MAVolatilityCode)
        MAVolatilityCode = FELib.iif((MAVolatilityCode == 0) & (ma_short < ma_long) & (ma_long - ma_short >= range1) & (ma_long - ma_short < range2)
                                     , 2, MAVolatilityCode)
        MAVolatilityCode = FELib.iif((MAVolatilityCode == 0) & (ma_short < ma_long) & (ma_long - ma_short >= range2) & (ma_long - ma_short < range3)
                                     , 3, MAVolatilityCode)
        MAVolatilityCode = FELib.iif((MAVolatilityCode == 0) & (ma_short < ma_long) & (ma_long - ma_short >= range3), 4, MAVolatilityCode)
        return MAVolatilityCode * 1.0

