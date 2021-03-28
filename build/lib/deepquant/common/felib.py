import numpy as np
import pandas as pd
import talib
from talib import MA_Type


class FELib:
    """
    Feature Engineering Library
    """

    @staticmethod
    def iif(condition, resultTrue, resultFalse):
        return np.where(condition, resultTrue, resultFalse)

    @staticmethod
    def llv(series, period):
        try:
            if type(series) == np.ndarray:
                series = pd.Series(series)
            series = series.fillna(0)
            result = series.rolling(period, min_periods=1).min()
            result = np.nan_to_num(result)
        except Exception as e:
            raise e
        return result

    @staticmethod
    def hhv(series, period):
        try:
            if type(series) == np.ndarray:
                series = pd.Series(series)
            # series = series.fillna(0)
            result = series.rolling(period, min_periods=1).max()
            result = np.nan_to_num(result)
        except Exception as e:
            raise e
        return result

    @staticmethod
    def ma(series, period):
        try:
            if type(series) == np.ndarray:
                series = pd.Series(series)
            series = series.fillna(0)
            result = series.rolling(period, min_periods=1).mean()
            result = np.nan_to_num(result)
        except Exception as e:
            raise e
        return result

    @staticmethod
    def stdev(series, period):
        try:
            if type(series) == np.ndarray:
                series = pd.Series(series)
            series = series.fillna(0)
            result = series.rolling(period, min_periods=1).std(skipna=True)
            result = np.nan_to_num(result)
        except Exception as e:
            raise e
        return result

    @staticmethod
    def shift(ndarr, period):
        e = np.empty_like(ndarr)
        if period >= 0:
            e[:period] = np.nan
            e[period:] = ndarr[:-period]
        else:
            e[period:] = np.nan
            e[:period] = ndarr[-period:]

        e = np.nan_to_num(e)
        return e

    @staticmethod
    def ref(series, steps):
        try:
            if type(series) == np.ndarray:
                series = pd.Series(series)
            result = FELib.shift(series, steps * -1)
        except Exception as e:
            raise e
        return result

    @staticmethod
    def cur(series):
        if type(series) == np.ndarray:
            series = pd.Series(series)
        series = series.fillna(0)
        result = series[len(series) - 1]
        result = np.nan_to_num(result)
        return result

    @staticmethod
    def llvbars(series, period):
        if type(series) == np.ndarray:
            series = pd.Series(series)
        series = series.fillna(0)
        min_idx = series.index[series.rolling(period).apply(np.argmin, raw=True)[(period - 1):].astype(int) + np.arange(len(series) - (period - 1))]
        out = [0] * len(series)
        out[len(out) - len(min_idx):len(out) + len(min_idx) - 1] = min_idx
        out = np.nan_to_num(out)
        return pd.Series(out)

    @staticmethod
    def hhvbars(series, period):
        if type(series) == np.ndarray:
            series = pd.Series(series)
        series = series.fillna(0)
        max_idx = series.index[series.rolling(period).apply(np.argmax, raw=True)[(period - 1):].astype(int) + np.arange(len(series) - (period - 1))]
        out = [0] * len(series)
        out[len(out) - len(max_idx):len(out) + len(max_idx) - 1] = max_idx
        out = np.nan_to_num(out)
        return pd.Series(out)

    @staticmethod
    def round_num(series, num):
        result = np.where(series % 1.0 == 0.5, (series + 0.5).round(num), series.round(num))
        return result

    # ==========================================================================================================================
    # ==========================================================================================================================
    @staticmethod
    def gen_feat_cdscolor(df, popen_col_name, pclose_col_name):
        df['CDS_COLOR'] = FELib.iif(df[popen_col_name] < df[pclose_col_name]
                                    , 1
                                    , FELib.iif(df[popen_col_name] > df[pclose_col_name]
                                                , 2, 3))

    @staticmethod
    def gen_feat_cdssize_abs(df, popen_col_name, pclose_col_name, cds_size_model):
        diff_arr = abs(np.array(df[popen_col_name]) - np.array(df[pclose_col_name]))
        diff_arr = diff_arr.reshape(-1, 1)
        df['CDS_SIZE'] = cds_size_model.predict(diff_arr)

    @staticmethod
    def gen_feat_hhv_cur_diff(df, col_name1, col_name2, period_list, price_range_model):
        for period in period_list:
            hhv = FELib.hhv(df[col_name1], period)
            diff_arr = abs(np.array(hhv) - np.array(df[col_name2]))
            diff_arr = diff_arr.reshape(-1, 1)
            level_arr = price_range_model.predict(diff_arr)
            level_arr = level_arr + 1
            df['MAX{}_{}_DIFF_{}'.format(period, col_name1, col_name2)] = FELib.iif(hhv > df[col_name2]
                                                                                    , level_arr
                                                                                    , 0)

    @staticmethod
    def gen_feat_cur_llv_diff(df, col_name1, col_name2, period_list, price_range_model):
        for period in period_list:
            llv = FELib.llv(df[col_name2], period)
            diff_arr = abs(np.array(col_name1) - np.array(df[llv]))
            diff_arr = diff_arr.reshape(-1, 1)
            level_arr = price_range_model.predict(diff_arr)
            level_arr = level_arr + 1
            df['{}_DIFF_MIN{}_{}'.format(col_name1, period, col_name2)] = FELib.iif(llv < df[col_name2]
                                                                                    , level_arr
                                                                                    , 0)

    @staticmethod
    def gen_feat_llv(df, col_name, period_list):
        feat_name = '_MIN_'
        for period in period_list:
            c_name = '{}{}{}'.format(col_name, feat_name, period)
            df[c_name] = FELib.llv(df[col_name], period)

    @staticmethod
    def gen_feat_hhv(df, col_name, period_list):
        feat_name = '_MAX_'
        for period in period_list:
            c_name = '{}{}{}'.format(col_name, feat_name, period)
            df[c_name] = FELib.hhv(df[col_name], period)

    @staticmethod
    def gen_feat_llvbars(df, col_name, period_list):
        feat_name = '_MINBARS_'
        for period in period_list:
            c_name = '{}{}{}'.format(col_name, feat_name, period)
            df[c_name] = FELib.llvbars(df[col_name], period)

    @staticmethod
    def gen_feat_hhvbars(df, col_name, period_list):
        feat_name = '_MAXBARS_'
        for period in period_list:
            c_name = '{}{}{}'.format(col_name, feat_name, period)
            df[c_name] = FELib.hhvbars(df[col_name], period)

    @staticmethod
    def gen_feat_sma(df, col_name, period_list):
        feat_name = '_AVG_'
        for period in period_list:
            c_name = '{}{}{}'.format(col_name, feat_name, period)
            df[c_name] = talib.SMA(df[col_name], period)

    def gen_feat_ema(df, col_name, period_list):
        feat_name = '_EMA_'
        for period in period_list:
            c_name = '{}{}{}'.format(col_name, feat_name, period)
            df[c_name] = talib.EMA(df[col_name], period)

    @staticmethod
    def gen_feat_is_gt(df, col_name1, col_name2):
        df['IS_GT_{}_{}'.format(col_name1, col_name2)] = FELib.iif(df[col_name1] > df[col_name2], 1, 0)

    @staticmethod
    def gen_feat_is_gt_val(df, col_name, value_list):
        for value in value_list:
            df['IS_GT_{}_{}'.format(col_name, value)] = FELib.iif(df[col_name] > value, 1, 0)

    @staticmethod
    def gen_feat_is_lt(df, col_name1, col_name2):
        df['IS_LT_{}_{}'.format(col_name1, col_name2)] = FELib.iif(df[col_name1] < df[col_name2], 1, 0)

    @staticmethod
    def gen_feat_is_lt_val(df, col_name, value_list):
        for value in value_list:
            df['IS_LT_{}_{}'.format(col_name, value)] = FELib.iif(df[col_name] < value, 1, 0)

    @staticmethod
    def gen_feat_break_high(df, col_name, period_list):
        feat_name = 'IS_BREAK_HIGH_{}_IN_{}'
        for period in period_list:
            c_name = feat_name.format(col_name, period)
            df[c_name] = FELib.hhv(df[col_name] == FELib.hhv(df[col_name]), 1, 0)

    @staticmethod
    def gen_feat_break_low(df, col_name, period_list):
        feat_name = 'IS_BREAK_LOW_{}_IN_{}'
        for period in period_list:
            c_name = feat_name.format(col_name, period)
            df[c_name] = FELib.hhv(df[col_name] == FELib.llv(df[col_name]), 1, 0)

