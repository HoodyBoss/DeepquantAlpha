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
                    , correl_id="NONE", st_bot="NONE"
                    , base_time="NONE", server_time="NONE", local_time="NONE"
                    , acc_number=0
                    , details="NONE")


class BaseStrategyRobotEndpoint():

    __config_path = "robot_config.yaml"

    def __init__(self, config, correl_id, base_time, server_time=None, local_time=None\
                 , account=None, positions=None, datasets=None, ml_models=None, **kwargs):
        """
        Initialize object
        :param config: a dictionary contains configuration about strategy, symbols, robots
        :param correl_id: a cross location transaction id
        :param base_time : a bar date time of smallest timeframe used by trading robot(s)
                            NOTE: each trading robot under same strategy robot can use different timeframe
                            this date time will be used in checking the trade time
                            example: assume a trading robot 'A' uses M15 and smallest timeframe
                                    used by strategy robot is M5,
                                    then latest bar time of M5 is 10:35 (that means current time is approx. 10:40)
                                    robot 'A' cannot run, the strategy robot will not call robot 'A'
                                    but if latest bar time of M5 is 10:40 (current time is approx. 10:45)
                                    robot 'A' can run, the strategy robot will call robot 'A'
                                    because current time or server time is approx. 10:45,
                                    the bar time of M15 is 10:30
        :param server_time: a date time on broker server
        :param local_time: a date time on server running frontend system (MT4/5, cTrader,...)
        :param account: a dictionary contains account info (only one account)
        :param positions: a dictionary of positions: key is trading robot name, value is dictionary of position's fields
                            In cTrader uses term 'position' but in MT4 uses 'order'. This system uses term 'position'
        :param datasets: a dictionary of model datasets, key is model name
        :param ml_models: a dictionary of ML models
        """
        self.config = config
        self.prev_correl_id = None     # Previous correlation ID, value will be set after executed strategy
        self.correl_id = correl_id
        self.strategy_name = self.config['strategy_name']
        self.base_time = base_time
        self.server_time = server_time
        self.local_time = local_time
        self.account = account
        self.positions = positions
        self.datasets = datasets
        self.ml_models = ml_models

        if self.account is not None and 'acc_number' in self.account:
            self.acc_number = self.account['acc_number']
        elif self.config['account_number'] is not None:
            self.acc_number = self.config['account_number']
        else:
            self.acc_number = 0

    def execute_strategy(self):
        try:
            # 1) Create strategy robot object using dynamic import
            class_strategy_robot = getattr( import_module(self.config['root_module_path'] + '.strategy_robot')\
                                            , 'StrategyRobot' )
            strategy_robot = class_strategy_robot(self.config, self.datasets, self.ml_models)

            # 2) Initialize strategy execution
            strategy_robot.init_execution(self.correl_id, self.base_time \
                                            , self.server_time, self.local_time, self.account, self.positions)

            # 3) Prepare strategy execution output
            """
            JSON schema of predict_results is:
            { "predict_results":    [   { "datetime":"xxx", "name": "xxx", "symbol_name":"xxx",... },
                                        { "datetime":"xxx", "name": "xxx", "symbol_name":"xxx",... }
                                    ]
            }
            """
            exec_output = {'correl_id' : str(self.correl_id)\
                        , 'strategy_name' : self.strategy_name
                        , 'base_time' : str(self.base_time)\
                        , 'server_time' : str(self.server_time)\
                        , 'local_time' : str(self.local_time)\
                        , 'predict_results' : []\
                        , 'predict_errors' : []}

            # 4) Run strategy execution
            strategy_robot.pre_execute(exec_output)
            strategy_robot.execute(exec_output)
            strategy_robot.post_execute(exec_output)

            # 5) Stamp previous correlation ID to current correlation ID, to be used for state checking
            self.prev_correl_id = self.correl_id

            dt = datetime.datetime.now().astimezone(utc_tz)
            log.info(time="{}".format(dt.isoformat())
                        , level="INFO", events="Call strategy execution"
                        , correl_id="{}".format(self.correl_id)
                        , st_bot=self.config['strategy_name']
                        , base_time="{}".format(self.base_time)
                        , server_time="{}".format(self.server_time), local_time="{}".format(self.local_time)
                        , acc_number="{}".format(self.acc_number)
                        , details="Execute successful")

            log.info(time="{}".format(dt.isoformat())
                     , level="INFO", events="Call strategy execution"
                     , correl_id="{}".format(self.correl_id)
                     , st_bot=self.config['strategy_name']
                     , base_time="{}".format(self.base_time)
                     , server_time="{}".format(self.server_time), local_time="{}".format(self.local_time)
                     , acc_number="{}".format(self.acc_number)
                     , details='exec_output in endpoint = {}'.format(exec_output))

        except error.StrategyRobotError as sre:
            err_msg = 'Call strategy execution error: {}'.format(sre)
            dt = datetime.datetime.now().astimezone(utc_tz)
            log.error(time="{}".format(dt.isoformat())
                     , level="ERROR", events="Call strategy execution"
                     , correl_id="{}".format(self.correl_id)
                     , st_bot=self.config['strategy_name']
                     , base_time="{}".format(self.base_time)
                     , server_time="{}".format(self.server_time), local_time="{}".format(self.local_time)
                     , acc_number="{}".format(self.acc_number)
                     , details=err_msg)
            raise Exception(err_msg)
        except Exception as e:
            err_msg = 'Unexpected error in calling strategy execution: {}'.format(e)
            dt = datetime.datetime.now().astimezone(utc_tz)
            log.error(time="{}".format(dt.isoformat())
                      , level="ERROR", events="Call strategy execution"
                      , correl_id="{}".format(self.correl_id)
                      , st_bot=self.config['strategy_name']
                      , base_time="{}".format(self.base_time)
                      , server_time="{}".format(self.server_time), local_time="{}".format(self.local_time)
                      , acc_number="{}".format(self.acc_number)
                      , details=err_msg)
            raise Exception()

        return exec_output
