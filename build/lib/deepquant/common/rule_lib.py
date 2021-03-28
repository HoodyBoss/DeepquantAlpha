import numpy as np


class RuleLib:

    @staticmethod
    def iif(condition, resultTrue, resultFalse):
        return np.where(condition, resultTrue, resultFalse)

    @staticmethod
    def llv(series, period):
        result = 0.0
        try:
            result = series.rolling(period, min_periods=1).min()
        except Exception as e:
            raise Exception('RuleUtil error - llv: {}'.format(e))
        return result

    @staticmethod
    def hhv(series, period):
        result = 0.0
        try:
            result = series.rolling(period, min_periods=1).max()
        except Exception as e:
            raise Exception('RuleUtil error - hhv: {}'.format(e))
        return result

    @staticmethod
    def ma(series, period):
        result = 0.0
        try:
            result = series.rolling(period, min_periods=1).mean()
        except Exception as e:
            raise Exception('RuleUtil error - ma: {}'.format(e))
        return result

    @staticmethod
    def stdev(series, period):
        result = 0.0
        try:
            result = series.rolling(period, min_periods=1).std(skipna=True)
        except Exception as e:
            raise Exception('RuleUtil error - stdev: {}'.format(e))
        return result

    @staticmethod
    def ref(series, steps):
        result = 0.0
        try:
            result = series.shift(steps * -1, fill_value=0.0)
        except Exception as e:
            raise Exception('RuleUtil error - ref: {}'.format(e))
        return result

    @staticmethod
    def cur(series):
        """
        Returns last value. It is a single value, not array.
        :param series:
        :return:
        """
        result = 0.0
        try:
            result = series[len(series) - 1]
        except Exception as e:
            raise Exception('RuleUtil error - cur: {}'.format(e))
        return result
