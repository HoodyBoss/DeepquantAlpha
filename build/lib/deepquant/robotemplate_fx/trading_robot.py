from importlib import import_module

import deepquant.common.error as error

from structlog import wrap_logger, PrintLogger
from structlog.processors import JSONRenderer
import datetime
import pytz
utc_tz = pytz.timezone('UTC')
logger = wrap_logger(PrintLogger(), processors=[JSONRenderer()])
log = logger.bind(time="NONE"
                    , level="INFO", events="NONE"
                    , correl_id="NONE", st_bot="NONE", tr_bot="NONE", acc_number=0
                    , details="NONE")


class BaseTradingRobot():

    # Initialize trading robot
    def __init__(self, robot_name, robot_context, robot_config):

        self.robot_name = robot_name
        # Create robot context จัดเก็บเป็น in-memory data โดยจัดเก็บอยู่ในหน่วยความจำ
        # robot context เปรียบเสมือนที่เก็บข้อมูลชั่วคราวระหว่างที่โรบอททำงาน
        self.robot_context = robot_context
        # Global configuration
        self.config = self.robot_context.config
        # Configuration for this trading robot
        self.robot_config = robot_config

        self.predict_result = None
        self.predict_error = None
        
        if self.robot_context.config is not None and self.robot_context.config['strategy_name'] is not None:
            self.strategy_name = self.robot_context.config['strategy_name']

        # The following attributes will be initialized in strategy robot
        self.correl_id = None
        self.acc_number = None


    def get_all_tr_model_ids(self, tr_models_conf):
        ids = []
        try:
            for tr_model in tr_models_conf:
                tr_model_id = tr_model['id']
                ids.append(tr_model_id)

            if self.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                        , level="DEBUG", events="Get all trading model ids"
                        , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                        , acc_number="{}".format(self.acc_number)
                        , details="Successful")
        except Exception as e:
            raise error.TradeRobotError('Get all trading model ids error: {}'.format(e))
        return ids


    def get_symbol_name(self, symbol_id):
        symbol_name = None
        try:
            for symbol in self.robot_context.config['symbols']:
                if symbol['id'] == symbol_id:
                    symbol_name = symbol['name']
                    break

            if self.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events="Get symbol name"
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , details="result: {}".format(symbol_name))
        except Exception as e:
            raise error.StrategyRobotError('Get symbol name error: {}'.format(e))
        return symbol_name


    def build_trade_model(self):
        trade_model = None

        try:
            tr_model_id = self.robot_config['trade_model_id']
            tr_model_module = self.robot_config['trade_model_module']
            tr_model_class = self.robot_config['trade_model_class']

            # Create trade model object using dynamic import
            class_trade_model = getattr(import_module(self.robot_context.config ['root_module_path'] + '.' + tr_model_module) \
                                        , tr_model_class)

            symbol_name = self.get_symbol_name(self.robot_config['symbol'])

            trade_model = class_trade_model(symbol_name \
                                            , self.robot_context \
                                            , self.robot_config \
                                            , tr_model_id)

            if self.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                      , level="DEBUG", events="Build trading model"
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , details="Successful")

        except Exception as e:
            raise error.TradeRobotError('Build trading model error: {}'.format(e))
        return trade_model


    def run(self):
        """
        Run trading robot as thread
        """
        try:
            # Build trading model
            trade_model = self.build_trade_model()

            if trade_model is not None:
                try:
                    # Call trading model to make prediction, output is either trading signal or action
                    result = trade_model.predict()
                    self.predict_result = result
                except Exception as e:
                    self.predict_error = '{}: {}'.format(self.robot_name, e)

                dt = datetime.datetime.now().astimezone(utc_tz)
                log.info(time="{}".format(dt.isoformat())
                          , level="INFO", events="Run trading model"
                          , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                          , acc_number="{}".format(self.acc_number)
                          , details="Run successful")

                if self.config['run_mode'] in ['debug', 'backtest']:
                    log.debug(time="{}".format(dt.isoformat())
                          , level="DEBUG", events="Run trading model"
                          , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                          , acc_number="{}".format(self.acc_number)
                          , details="result: {}".format(self.predict_result))

            else:
                err_msg = "Zero trading model error: trading robot must has at least one trading model"
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.error(time="{}".format(dt.isoformat())
                         , level="ERROR", events="Run trading model"
                         , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                         , acc_number="{}".format(self.acc_number)
                         , details=err_msg)
                raise error.TradeRobotError("Zero trading model error: trading robot must has at least one trading model")

        except Exception as e:
            err_msg = 'Execute trading robot error: {}'.format(e)
            dt = datetime.datetime.now().astimezone(utc_tz)
            log.error(time="{}".format(dt.isoformat())
                      , level="ERROR", events="Run trading model"
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name, tr_bot=self.robot_name
                      , acc_number="{}".format(self.acc_number)
                      , details=err_msg)
            raise error.TradeRobotError(err_msg)

        # Build result
        result = {'predict_result':self.predict_result, 'predict_error':self.predict_error}
        return result
