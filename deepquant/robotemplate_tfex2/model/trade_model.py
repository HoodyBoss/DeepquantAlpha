import numpy as np

import deepquant.common.error as error
import deepquant.common.state_machine as st_machine
import deepquant.common.global_cons as cons
from deepquant.common.timeframe import Timeframe
from deepquant.common.felib import FELib

import deepquant.robotemplate_fx.model.model_helper as model_helper

from structlog import wrap_logger, PrintLogger
from structlog.processors import JSONRenderer
import datetime
import pytz
utc_tz = pytz.timezone('UTC')
logger = wrap_logger(PrintLogger(), processors=[JSONRenderer()])
log = logger.bind(time="NONE"
                    , level="INFO", events="NONE"
                    , correl_id="NONE", st_bot="NONE", tr_bot="NONE", acc_number=0
                    , trade_model_name="NONE", symbol_name="NONE"
                    , details="NONE")

class BaseTradeModel:

    def __init__(self, trade_model_name, symbol_name, robot_context, robot_config, trade_model_id=None):

        self.signal_cols = ['DATETIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME' \
                            , 'OPEN_BUY', 'OPEN_SELL', 'CLOSE_BUY', 'CLOSE_SELL' \
                            , 'STOP_LOSS', 'TAKE_PROFIT', 'POS_SIZE' \
                            , 'SCALE_TYPE', 'SCALE_SIZE', 'STOP_TYPE']

        self.point_value = 200  # 200 BAHT / 1 point
        self.pos_size_decimal_num = 0  # position size (contract) must be integer
        self.min_pos_size = 1 # minimum position size (contract) is 1

        self.dataset_row_length = 0 # NOTE: MUST Define this value by compute from dataset

        # Static state data
        self.state_machine = st_machine.StateMachine()
        self.default_trade_signal = self.state_machine.SIGNAL_NONE
        self.trade_model_name = trade_model_name
        self.symbol_name = symbol_name
        self.robot_context = robot_context
        self.robot_config = robot_config
        self.trade_model_id = trade_model_id

        if self.robot_context.config is not None and self.robot_context.config['strategy_name'] is not None:
            self.strategy_name = self.robot_context.config['strategy_name']
        else:
            self.strategy_name = "NONE"

        if self.robot_config is not None and 'name' in self.robot_config:
            self.robot_name = self.robot_config['name']
        else:
            self.robot_name = 'Anonymous'

        # Dynamic state data
        # predictive model object, not ML model
        self.models = {}
        # Dictionary object, key is model name defined in 'models' in trading robot config, value type is DataFrame
        self.datasets = self.robot_context.datasets

        self.correl_id = self.robot_context.correl_id
        self.account = self.robot_context.account
        self.position = self.robot_context.position
        if self.account is not None and 'acc_number' in self.account:
            self.acc_number = self.account['acc_number']
        else:
            self.acc_number = 0

        # Initialize indicators. This is useful for rule based model.
        self.init_indi(self.datasets[self.trade_model_name])

    def init_indi(self, dataset_df):
        pass

    def prepare_datetime(self, dataset_df):
        if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
            dt = datetime.datetime.now().astimezone(utc_tz)
            log.debug(time="{}".format(dt.isoformat())
                    , level="DEBUG", events="Prepare datetime"
                    , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                    , acc_number="{}".format(self.acc_number)
                    , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                    , details='dataset_df.columns = {}'.format(list(dataset_df.columns)))

        try:
            if 'time' in list(dataset_df.columns):
                if type(dataset_df['time'].iloc[0]) != str:
                    dataset_df['date'] = (dataset_df['date'] * 1000000) + dataset_df['time']
                    dataset_df['date'] = dataset_df['date'].astype(str)
                else:
                    dataset_df['date'] = dataset_df['date'] + dataset_df['time']
                dataset_df = dataset_df.drop(columns=['time'])
            elif 'TIME' in dataset_df.columns:
                if type(dataset_df['TIME'].iloc[0]) == str:
                    dataset_df['DATETIME'] = (dataset_df['DATE'].astype(int) * 1000000) + dataset_df['TIME'].astype(int)
                    dataset_df['DATETIME'] = dataset_df['DATETIME'].astype(str)
                else:
                    dataset_df['DATETIME'] = dataset_df['DATE'] + dataset_df['TIME']
                dataset_df = dataset_df.drop(columns=['TIME'])

            dataset_df.columns = dataset_df.columns.str.strip().str.upper().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

            if 'volume' in dataset_df.columns or 'VOLUME' in dataset_df.columns:
                self.contain_volume = True
            else:
                self.contain_volume = False
                dataset_df['VOLUME'] = 0.0

            #dataset_df.columns = ['DATETIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
            if 'date' in list(dataset_df.columns):
                dataset_df = dataset_df.rename(columns={'date': "DATETIME"})
            elif 'DATE' in list(dataset_df.columns):
                dataset_df = dataset_df.rename(columns={'DATE': "DATETIME"})

        except Exception as e:
            err_msg = 'Trade model error: {}'.format(e)
            dt = datetime.datetime.now().astimezone(utc_tz)
            log.error(time="{}".format(dt.isoformat())
                      , level="ERROR", events="Prepare datetime"
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details=err_msg)
            raise error.TradeModelError(err_msg)
        return dataset_df

    def get_trade_type_code(self, trade_type):
        if trade_type is not None and trade_type.upper() == cons.BUY or cons.LONG:
            trade_type_code = 1
        elif trade_type is not None and trade_type.upper() == cons.SELL or cons.SHORT:
            trade_type_code = 2
        return trade_type_code

    def get_symbol_info(self, symbol_name):
        """
        Returns a dictionary of symbol info
        :param symbol_name:
        :return: a dictionary of symbol info
        """
        symbol_info = None
        for symbol in self.robot_context.config['symbols']:
            if symbol['name'] == self.symbol_name:
                symbol_info = symbol
                break
        return symbol_info


    # ==============================================================================================================
    # Alpha Model Methods:
    # ==============================================================================================================
    # ******************************************************************************************************************************
    # ==============================================================================================================================
    # BEGIN: Predict Open Buy/Sell
    # ==============================================================================================================================
    def predict_open_buy(self, **kwargs):
        sig = np.zeros((self.dataset_row_length, 1))

        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                        , level="DEBUG", events='Predict open buy signal'
                        , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                        , acc_number="{}".format(self.acc_number)
                        , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                        , details="result: {}".format(sig))
        except Exception as e:
            raise error.TradeModelError('Predict open buy signal error: {}'.format(e))

        return sig

    def predict_open_sell(self, **kwargs):
        sig = np.zeros((self.dataset_row_length, 1))

        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                        , level="DEBUG", events='Predict open sell signal'
                        , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                        , acc_number="{}".format(self.acc_number)
                        , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                        , details="result: {}".format(sig))
        except Exception as e:
            raise error.TradeModelError('Predict open sell signal error: {}'.format(e))

        return sig

    def predict_open_sig(self, **kwargs):
        sig = np.zeros((self.dataset_row_length, 1))

        try:
            # Add code here
            """
            # The following code is an example when using ML
            barracuda = copy.deepcopy(self.models['barracuda'])  # Copy ML model object
            feature_first_col = 7
            feature_last_col = len(self.model_datasets['barracuda'].columns)  # dataset type is DataFrame
            sig = barracuda.predict(self.model_datasets['barracuda'] \
                                                         , feature_first_col, feature_last_col, 'all')
            """

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                        , level="DEBUG", events='Predict open signal'
                        , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                        , acc_number="{}".format(self.acc_number)
                        , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                        , details="result: {}".format(sig))
        except Exception as e:
            raise error.TradeModelError('Predict open signal error: {}'.format(e))

        return sig

    # ==============================================================================================================================
    # END: Predict Open Buy/Sell
    # ==============================================================================================================================

    # ******************************************************************************************************************************
    # ==============================================================================================================================
    # BEGIN: Predict Close Buy/Sell
    # ==============================================================================================================================
    def predict_close_buy(self, **kwargs):
        sig = np.zeros((self.dataset_row_length, 1))

        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Predict close buy signal'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(sig))
        except Exception as e:
            raise error.TradeModelError('Predict close buy error: {}'.format(e))

        return sig

    def predict_close_sell(self, **kwargs):
        sig = np.zeros((self.dataset_row_length, 1))

        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Predict close sell signal'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(sig))
        except Exception as e:
            raise error.TradeModelError('Predict close sell error: {}'.format(e))

        return sig

    def predict_close_sig(self, **kwargs):
        sig = np.zeros((self.dataset_row_length, 1))

        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Predict close signal'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(sig))
        except Exception as e:
            raise error.TradeModelError('Predict close signal error: {}'.format(e))

        return sig
    # ==============================================================================================================================
    # END: Predict Close Buy/Sell
    # ==============================================================================================================================

    # ******************************************************************************************************************************
    # ==============================================================================================================================
    # BEGIN: Filter Open Buy/Sell
    # ==============================================================================================================================
    def filter_open_buy(self, init_signal, **kwargs):
        sig = init_signal

        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Filter open buy signal'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(sig))
        except Exception as e:
            raise error.TradeModelError('Filter open buy error: {}'.format(e))

        return sig

    def filter_open_sell(self, init_signal, **kwargs):
        sig = init_signal

        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Filter open sell signal'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(sig))
        except Exception as e:
            raise error.TradeModelError('Filter open sell error: {}'.format(e))

        return sig

    # ==============================================================================================================================
    # END: Filter Open Buy/Sell
    # ==============================================================================================================================

    # ******************************************************************************************************************************
    # ==============================================================================================================================
    # BEGIN: Filter Close Buy/Sell
    # ==============================================================================================================================
    def filter_close_buy(self, init_signal, **kwargs):
        sig = init_signal

        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Filter close buy signal'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(sig))
        except Exception as e:
            raise error.TradeModelError('Filter close buy error: {}'.format(e))

        return sig

    def filter_close_sell(self, init_signal, **kwargs):
        sig = init_signal

        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Filter close sell signal'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(sig))
        except Exception as e:
            raise error.TradeModelError('Filter close sell error: {}'.format(e))

        return sig

    # ==============================================================================================================================
    # END: Filter Close Buy/Sell
    # ==============================================================================================================================

    # ******************************************************************************************************************************
    # ==============================================================================================================================
    # BEGIN: Predict Stop Loss
    # ==============================================================================================================================
    def predict_stop_loss(self, sig_open_buy, sig_open_sell, **kwargs):
        sl = np.zeros((self.dataset_row_length, 1))

        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Predict stop loss'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(sl))
        except Exception as e:
            raise error.TradeModelError('Predict stop loss error: {}'.format(e))

        return sl

    # ==============================================================================================================================
    # BEGIN: Predict Stop Loss
    # ==============================================================================================================================

    # ******************************************************************************************************************************
    # ==============================================================================================================================
    # BEGIN: Predict Take Profit
    # ==============================================================================================================================
    def predict_take_profit(self, sig_open_buy, sig_open_sell, **kwargs):
        tp = np.zeros((self.dataset_row_length, 1))

        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Predict take profit'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(tp))
        except Exception as e:
            raise error.TradeModelError('Predict take profit error: {}'.format(e))

        return tp

    # ==============================================================================================================================
    # END: Predict Take Profit
    # ==============================================================================================================================

    # ******************************************************************************************************************************
    # ==============================================================================================================================
    # BEGIN: Predict Entry Position Size
    # ==============================================================================================================================
    def predict_entry_pos_size_pct(self, sig_open_buy, sig_open_sell, **kwargs):
        posSize = np.zeros((self.dataset_row_length, 1))

        try:
            posSize = 1.0 # Replace code in this try - except block

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Predict entry position size'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(posSize))
        except Exception as e:
            raise error.TradeModelError('Predict entry position size (%) error: {}'.format(e))

        return posSize

    def cal_entry_pos_size(self, **kwargs):
        pos_size = 0.0

        try:
            balance = self.robot_context.config['reserve_fund'] + self.account['balance']
            pos_size = model_helper.cal_entry_pos_size(stop_loss=kwargs['stop_loss']
                                                    , balance=balance
                                                    , base_risk=self.robot_config['base_risk']
                                                    , fund_allocate_size=self.robot_config['fund_allocate_size']
                                                    , cal_pos_size_formula=self.robot_config['cal_pos_size_formula']
                                                    , limit_pos_size=self.robot_config['limit_pos_size']
                                                    , cal_pos_size_formula2_size=self.robot_config['cal_pos_size_formula2_size']
                                                    , qty_percent=kwargs['qty_percent']
                                                    , pos_size_decimal_num=self.pos_size_decimal_num
                                                    , point_value=self.point_value)
        except Exception as e:
            raise error.TradeModelError('Calculate entry position size error: {}'.format(e))

        if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
            dt = datetime.datetime.now().astimezone(utc_tz)
            log.debug(time="{}".format(dt.isoformat())
                  , level="DEBUG", events='Calculate entry position size'
                  , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                  , acc_number="{}".format(self.acc_number)
                  , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                  , details="result: {}".format(pos_size))

        return pos_size

    def validate_pos_size_limit(self, **kwargs):
        """
        ตรวจสอบค่า position size limit ว่ามีขนาดเกินจากที่กำหนดไว้ในคอนฟิกฯ หรือไม่
        :param kwargs: 'volume', data type is float
        :return: ค่า position size เดิม หากไม่เกินลิมิต แต่ถ้าเกินจะ return ค่า limit แทน
        """
        pos_size = kwargs['volume']
        limit = self.robot_config['limit_pos_size']

        if pos_size > limit :
            pos_size = limit

        if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
            dt = datetime.datetime.now().astimezone(utc_tz)
            log.debug(time="{}".format(dt.isoformat())
                  , level="DEBUG", events='Validate position size'
                  , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                  , acc_number="{}".format(self.acc_number)
                  , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                  , details="result: {}".format(pos_size))

        return pos_size

    # ==============================================================================================================================
    # BEGIN: Predict Entry Position Size
    # ==============================================================================================================================



    # ==============================================================================================================================
    # BEGIN: Methods for Holding Position
    # ==============================================================================================================================
    def predict_scale_out(self, **kwargs):
        scale_size = 0.0
        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Predict scale out'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(scale_size))
        except Exception as e:
            raise error.TradeModelError('Predict scale out error: {}'.format(e))

        return scale_size

    def predict_scale_in(self, **kwargs):
        scale_size = 0.0
        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Predict scale in'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(scale_size))
        except Exception as e:
            raise error.TradeModelError('Predict scale in error: {}'.format(e))

        return scale_size

    def predict_modify_stop_loss(self, **kwargs):
        sl = 0.0

        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Predict modify stop loss'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(sl))
        except Exception as e:
            raise error.TradeModelError('Predict modify stop loss error: {}'.format(e))

        return sl

    def predict_modify_take_profit(self, **kwargs):
        tp = 0.0

        try:
            # Add code here

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Predict modify take profit'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(tp))
        except Exception as e:
            raise error.TradeModelError('Predict modify take profit error: {}'.format(e))

        return tp

    def predict_stop_by_conditions(self, **kwargs):
        stop_type = 0

        try:
            # Add code in the following

            # Predict stop by max loss
            # stop_type = cons.STOP_MAX_LOSS

            # Predict stop by trailing stop
            # stop_type = cons.STOP_TRAIL

            # Predict stop by profit
            # stop_type = cons.STOP_PROFIT

            # Predict stop by time
            # stop_type = cons.STOP_TIME

            # Predict stop by other conditions
            # stop_type = cons.STOP_OTHERS

            if self.robot_context.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events='Predict stop by conditions'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details="result: {}".format(stop_type))
        except Exception as e:
            raise error.TradeModelError('Predict trailing stop error: {}'.format(e))

        return stop_type
    # ==============================================================================================================================
    # END: Methods for Holding Position
    # ==============================================================================================================================



    # ==============================================================================================================================
    # BEGIN: Main Method for Predict Signal and Action
    # ==============================================================================================================================
    def predict(self, output_type=2):
        """
        Make prediction: ฟังก์ชั่นนี้จะทำการ predict ผลลัพธ์ โดยจะ return ค่าอย่างใดอย่างหนึ่งขึ้นกับค่า output_type ที่ระบุมา
                            - หาก output_type เท่ากับ 1 จะ return ค่า signals DataFrame โดยมีจำนวน row เท่ากับจำนวน row ตั้งต้น
                            หรือจำนวนบาร์ของข้อมูลราคา
                            *สำหรับค่า position size และ scale size จะใช้เป็นค่าเปอร์เซ็นต์ ไม่ใช่ค่าขนาดที่จะใช้เทรดจริง
                            เช่น 1.0 หมายถึง 100%, 0.3 หมายถึง 30%
                            - หาก output_type เท่ากับ 2 จะ return ค่า trading action ของบาร์ล่าสุดบาร์เดียว
                            *สำหรับค่า position size และ scale size จะใช้เป็นค่าขนาดจริง เช่น 2.0 หมายถึง 2 lot หรือ 2 สัญญา
        :param output_type: can be 1 for signals (DataFrame) or 2 for action of last bar (dictionary type)
        :return: signals DataFrame or action of last bar
        """
        output = None
        sm = self.state_machine

        try:
            """
            BEGIN: Predict Signal
            """
            init_sig_open_buy = self.predict_open_buy()
            init_sig_open_sell = self.predict_open_sell()
            init_sig_close_buy = self.predict_close_buy()
            init_sig_close_sell = self.predict_close_sell()

            final_sig_open_buy = self.filter_open_buy(init_sig_open_buy)
            final_sig_open_sell = self.filter_open_sell(init_sig_open_sell)
            final_sig_close_buy = FELib.iif((self.filter_close_buy(init_sig_close_buy) == 1) | (final_sig_open_sell == 1), 1, 0)
            final_sig_close_sell = FELib.iif((self.filter_close_sell(init_sig_close_sell) == 1) | (final_sig_open_buy == 1), 1, 0)

            sig_stop_loss = self.predict_stop_loss(final_sig_open_buy, final_sig_open_sell)
            sig_take_profit = self.predict_take_profit(final_sig_open_buy, final_sig_open_sell)
            sig_pos_size_pct = self.predict_entry_pos_size_pct(final_sig_open_buy, final_sig_open_sell)
            """
            END: Predict Signal
            """

            if output_type == 1:
                self.signal['OPEN_BUY'] = final_sig_open_buy
                self.signal['OPEN_SELL'] = final_sig_open_sell
                self.signal['CLOSE_BUY'] = final_sig_close_buy
                self.signal['CLOSE_SELL'] = final_sig_close_sell

                self.signal['STOP_LOSS'] = sig_stop_loss
                self.signal['TAKE_PROFIT'] = sig_take_profit
                self.signal['POS_SIZE'] = sig_pos_size_pct

                # Set default values because this model does not use scaling and trailing stop
                self.signal['SCALE_TYPE'] = 0
                self.signal['SCALE_SIZE'] = 0.0
                self.signal['STOP_TYPE'] = 0

                original_cols = self.signal.columns
                drop_cols = list()
                # Drop unwanted columns
                for i in range(0, len(original_cols)):
                    if original_cols[i] not in self.signal_cols:
                        drop_cols.append(original_cols[i])
                self.signal = self.signal.drop(drop_cols, axis=1)

                # Assign dataframe to output
                output = self.signal

            elif output_type == 2:
                output = {}

                #p_close = self.model_datasets['barracuda']['close']
                p_close = self.arrClose
                symbol_info = self.get_symbol_info(self.symbol_name)

                act_signal_code = sm.SIGNAL_NONE
                act_action_code = sm.ACTION_WAIT
                act_stop_type = 0
                act_scale_size = 0.0
                act_stop_loss = 0.0         # Price
                act_take_profit = 0.0       # Price
                act_stop_loss_pips = 0.0    # Pip
                act_take_profit_pips = 0.0  # Pip
                act_quantity = 0.0
                act_label = ''
                act_comment = 'NONE'
                act_trade_type = 'IDLE'
                act_has_trailing_stop = 'False'
                act_stop_loss_trigger_method = 'NONE'

                # Predict new action for holding position(s)
                if self.position is not None:

                    act_trade_type = self.position['trade_type']
                    if act_trade_type is not None and act_trade_type.upper() in [cons.BUY, cons.SELL]:
                        trade_type_code = self.get_trade_type_code(act_trade_type)

                        # Predict stop by conditions: ดูว่าควรปิดสถานะด้วย stop แล้วหรือไม่ เงื่อนไขเช่น profit/loss, time
                        act_stop_type = self.predict_stop_by_conditions()

                        if act_stop_type in [cons.STOP_MAX_LOSS, cons.STOP_TRAIL, cons.STOP_PROFIT, cons.STOP_TIME, cons.STOP_OTHERS]:
                            act_signal_code = sm.SIGNAL_CLOSE_AND_WAIT

                        # Predict หาค่าใหม่ของ stop loss เช่นในการทำ trailing stop
                        if act_signal_code == sm.SIGNAL_NONE:
                            act_stop_loss_pips = self.predict_modify_stop_loss()
                            if act_stop_loss_pips != 0.0:
                                act_stop_loss = self.get_sl_price(p_close[-1] \
                                                                    , act_stop_loss_pips, act_trade_type \
                                                                    , symbol_info['tick_size'], symbol_info['digits'])
                                act_signal_code = sm.SIGNAL_MODIFY_STOP_LOSS

                        # Predict หาค่าใหม่ของ take profit เช่นในการขยับระยะทำกำไรเข้าหรือออก
                        if act_signal_code == sm.SIGNAL_NONE:
                            act_take_profit_pips = self.predict_modify_take_profit()
                            if act_take_profit != 0.0:
                                act_take_profit = self.get_tp_price(p_close[-1] \
                                                                    , act_take_profit_pips, act_trade_type \
                                                                    , symbol_info['tick_size'], symbol_info['digits'])
                                act_signal_code = sm.SIGNAL_MODIFY_TAKE_PROFIT

                        # Predict scale out: ดูว่าควรปิดสถานะบางส่วนหรือไม่ เช่น ได้กำไรถึงเป้า, ถือมานานแล้ว, ขาดทุนอยู่
                        if act_signal_code == sm.SIGNAL_NONE:
                            scale_size_pct = self.predict_scale_out()
                            if scale_size_pct != 0.0:
                                act_quantity = round(self.position['quantity'] * scale_size_pct, self.pos_size_decimal_num)
                                act_signal_code = sm.SIGNAL_SCALE_OUT

                        # NOTE: trading model นี้ไม่มีการทำ scale in

                        act_action_code = sm.get_trade_action(act_signal_code, trade_type_code)
                        if act_signal_code == sm.SIGNAL_NONE and act_action_code in [sm.ACTION_HOLD_BUY, sm.ACTION_HOLD_SELL]:
                            act_stop_loss_pips = self.position['stop_loss_pips'] if self.position['stop_loss_pips'] > 0 else 0.0
                            act_take_profit_pips = self.position['take_profit_pips'] if self.position['take_profit_pips'] > 0 else 0.0
                            act_stop_loss = self.position['stop_loss'] if self.position['stop_loss_pips'] > 0 else 0.0
                            act_take_profit = self.position['take_profit'] if self.position['take_profit'] > 0 else 0.0
                            act_label = self.position['label']
                            act_quantity = self.position['quantity']

                    dt = datetime.datetime.now().astimezone(utc_tz)
                    log.info(time="{}".format(dt.isoformat())
                            , level="INFO", events='Predict signal/action'
                            , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                            , acc_number="{}".format(self.acc_number)
                            , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                            , details="Predict signal/action for active position '{}' successful".format(self.position['label']))

                else: # Predict new signal and action for new position

                    act_signal_code = sm.SIGNAL_BUY if final_sig_open_buy[-1] == 1 else act_signal_code
                    act_signal_code = sm.SIGNAL_SELL if final_sig_open_sell[-1] == 1 else act_signal_code

                    if self.account is not None:
                        trading_robot_id = self.robot_config['id']

                        # Create position label
                        strategy_id = str(self.robot_context.config['strategy_id'])
                        robot_id = str(trading_robot_id)
                        symbol_id = str(symbol_info['id'])
                        timeframe_id = str(Timeframe.get_code(self.robot_config['timeframe']))
                        act_label = model_helper.generate_trade_label(strategy_id=strategy_id, robot_id=robot_id\
                                            , symbol_id=symbol_id, trade_model_id=self.trade_model_id)

                        # Set action code and required fields
                        if act_signal_code == sm.SIGNAL_BUY:
                            act_action_code = sm.ACTION_OPEN_BUY
                            act_trade_type = cons.BUY

                        elif act_signal_code == sm.SIGNAL_SELL:
                            act_action_code = sm.ACTION_OPEN_SELL
                            act_trade_type = cons.SELL

                        if act_action_code in [sm.ACTION_OPEN_BUY, sm.ACTION_OPEN_SELL]:
                            # Calculate position size (not percentage)
                            qty_percent = sig_pos_size_pct[-1] / 100.0
                            act_quantity = self.cal_entry_pos_size(qty_percent=qty_percent, stop_loss=sig_stop_loss[-1])
                            # กรณีเงินในพอร์ตฯ ไม่พอ หรือโมเดล predict ขนาด pos. size ออกมาเล็กมากจนมีค่าน้อยกว่า 0.01
                            # แก้ไขโดยเพิ่ม attribute ชื่อ min_pos_size ให้มีค่าเท่ากับ 0.01
                            # กรณีเกิดเหตุการณ์ดังกล่าวจะใช้ค่า quantity (pos. size หรือ Lot) เท่ากับ 0.01
                            if act_quantity == 0.0:
                                act_quantity = self.min_pos_size

                            act_stop_loss_pips = sig_stop_loss[-1]
                            act_take_profit_pips = sig_take_profit[-1]

                            act_stop_loss = 0.0 if sig_stop_loss[-1] < 0 else self.get_sl_price(p_close[-1] \
                                                                , act_stop_loss_pips, act_trade_type \
                                                                , symbol_info['tick_size'], symbol_info['digits'])
                            act_take_profit = 0.0 if sig_take_profit[-1] < 0 else self.get_tp_price(p_close[-1] \
                                                                , act_take_profit_pips, act_trade_type \
                                                                , symbol_info['tick_size'], symbol_info['digits'])
                        else:
                            sig_pos_size_pct[-1] = 0.0

                    dt = datetime.datetime.now().astimezone(utc_tz)
                    log.info(time="{}".format(dt.isoformat())
                             , level="INFO", events='Predict signal/action'
                             , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                             , acc_number="{}".format(self.acc_number)
                             , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                             , details="Predict signal/action for new position successful")

                # Build trading action
                # position in Settrade Open API has no ID
                #output['id'] = int(self.position['id']) if self.position is not None else 0 # position (order) ID

                output['datetime'] = str(self.arrDatetime.iloc[-1]) # Bar date time, not timestamp and server time
                output['robot_name'] = self.robot_name # Trading robot name
                output['symbol_name'] = self.symbol_name
                output['signal_code'] = act_signal_code
                output['action_code'] = act_action_code

                if act_signal_code is not None:
                    output['signal_name'] = sm.get_signal_name(act_signal_code)
                if act_action_code is not None:
                    output['action_name'] = sm.get_action_name(act_action_code)

                output['stop_type'] = act_stop_type
                output['scale_size'] = act_scale_size
                output['stop_loss'] = round(act_stop_loss, symbol_info['digits'])
                output['take_profit'] = round(act_take_profit, symbol_info['digits'])
                output['stop_loss_pips'] = round(act_stop_loss_pips, 1)
                output['take_profit_pips'] = round(act_take_profit_pips, 1)
                output['entry_quantity_percent'] = round(sig_pos_size_pct[-1], 2)
                output['quantity'] = round(act_quantity, self.pos_size_decimal_num)
                output['label'] = str(act_label)
                output['comment'] = act_comment
                output['trade_type'] = str(act_trade_type)
                output['has_trailing_stop'] = act_has_trailing_stop
                output['stop_loss_trigger_method'] = act_stop_loss_trigger_method

                log.info(time="{}".format(datetime.datetime.now().astimezone(utc_tz).isoformat())
                        , level="DEBUG", events='Predict signal/action'
                        , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                        , acc_number="{}".format(self.acc_number)
                        , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                        , details="Built prediction result successful: {}".format(output))

        except Exception as e:
            err_msg = 'Run prediction in Trading model error: {}'.format(e)
            dt = datetime.datetime.now().astimezone(utc_tz)
            log.error(time="{}".format(dt.isoformat())
                      , level="ERROR", events='Predict signal/action'
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , trade_model_name="{}".format(self.trade_model_name), symbol_name=self.symbol_name
                      , details=err_msg)
            raise error.TradeModelError(err_msg)

        return output

    def get_sl_price(self, entry_price, sl_pips, trade_type, tick_size, symbol_digits,):
        price = 0.0
        if sl_pips > 0:
            if trade_type.upper() == cons.BUY:
                price = self.cal_price(entry_price, sl_pips * -1, tick_size)
            elif trade_type.upper() == cons.SELL:
                price = self.cal_price(entry_price, sl_pips, tick_size)

            price = round(price, symbol_digits)
        return price

    def get_tp_price(self, entry_price, tp_pips, trade_type, tick_size, symbol_digits):
        price = 0.0
        if tp_pips > 0:
            if trade_type.upper() == cons.BUY:
                price = self.cal_price(entry_price, tp_pips, tick_size)
            elif trade_type.upper() == cons.SELL:
                price = self.cal_price(entry_price, tp_pips * -1, tick_size)

            price = round(price, symbol_digits)
        return price

    def cal_price(self, price, pips, tick_size):
        new_price = price + (pips * tick_size)
        return new_price
    # ==============================================================================================================================
    # END: Main Method for Predict Signal and Action
    # ==============================================================================================================================

