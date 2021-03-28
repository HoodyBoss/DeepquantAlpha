import multiprocessing
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
                    , correl_id="NONE", st_bot="NONE", acc_number=0
                    , details="NONE")


class BaseStrategyRobot:

    def __init__(self, config, datasets=None, ml_models=None):
        self.config = config
        self.datasets = datasets
        self.ml_models = ml_models
        self.strategy_name = self.config['strategy_name']

        try:
            # 1) Load trading robots
            self.trading_robots = self.load_trading_robots()
            # 2) Create empty prediction result dictionary and empty prediction error dictionary, key is name of trading robot
            self.trbot_predict_results = {}
            self.trbot_predict_errors = {}
            # 3) Add empty prediction result list and empty error list to each trading robot
            #self.add_empty_list(self.trading_robots)

            if self.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                        , level="DEBUG", events="Initialize strategy robot"
                        , st_bot=self.strategy_name
                        , acc_number="{}".format(self.config['account_number'])
                        , details="Initialize successful")
        except Exception as e:
            dt = datetime.datetime.now().astimezone(utc_tz)
            log.error(time="{}".format(dt.isoformat())
                        , level="ERROR", events="Initialize strategy robot"
                        , st_bot=self.strategy_name
                        , acc_number="{}".format(self.config['account_number'])
                        , details="Initialize error: {}".format(e))
            raise error.StrategyRobotError('Initialize strategy robot object error: {}'.format(e))

    def init_execution(self, correl_id, base_time, server_time=None, local_time=None, account=None, positions=None):
        self.correl_id = correl_id
        if account is not None and 'acc_number' in account:
            self.acc_number = account['acc_number']
        elif self.config['account_number'] is not None:
            self.acc_number = self.config['account_number']
        else:
            self.acc_number = 0

        try:
            for tr_robot_key in list(self.trading_robots.keys()):
                tr_robot = self.trading_robots[tr_robot_key]
                # Validate trade time for each trading robot. All trading robots MUST use same time frame
                tr_robot_tf = tr_robot.robot_config['timeframe']

                if self.config['enable_check_trade_time'] == True:
                    allow_trade = self.is_trade_time(base_time, tr_robot_tf)
                else:
                    allow_trade = True

                if allow_trade == True:
                    # Get position related to each trading robot name
                    tr_bot_position = None
                    if positions is not None and tr_robot.robot_name in list(positions.keys()):
                        tr_bot_position = positions[tr_robot.robot_name]

                    tr_robot.robot_context.correl_id = self.correl_id
                    tr_robot.robot_context.base_time = base_time
                    tr_robot.robot_context.account = account
                    tr_robot.robot_context.position = tr_bot_position
                    tr_robot.robot_context.server_time = server_time
                    tr_robot.robot_context.local_time = local_time

                    # The following will be used for logging
                    tr_robot.correl_id = self.correl_id
                    tr_robot.acc_number = self.acc_number
                # End if
            # End for

            dt = datetime.datetime.now().astimezone(utc_tz)
            log.info(time="{}".format(dt.isoformat())
                      , level="INFO", events="Initialize strategy execution"
                      , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name
                      , acc_number="{}".format(self.acc_number)
                      , details="Initialize successful")

        except Exception as e:
            err_msg = 'Initialize strategy execution error: {}'.format(e)
            dt = datetime.datetime.now().astimezone(utc_tz)
            log.error(time="{}".format(dt.isoformat())
                     , level="INFO", events="Initialize strategy execution"
                     , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name
                     , acc_number="{}".format(self.acc_number)
                     , details=err_msg)
            raise error.StrategyRobotError(err_msg)

    def is_trade_time(self, base_time, tr_robot_tf):
        result = False
        try:
            # Get base timeframe, the base timeframe is a main timeframe
            # while other robot's timeframes can use bigger than this
            # NOTE: FOR THIS VERSION, ALL TRADING ROBOTS MUST USE SAME TIMEFRAME !!!
            base_tf = self.config['base_timeframe']
            base_tf_num = int(base_tf[1:len(base_tf)])

            # Get bar hour and minute
            # Format of base_time is YYYYMMddHHmmss, for example: 20191120143000
            bar_hour = int(base_time[8:10])
            bar_minute = int(base_time[10:12])

            # Get trading robot timeframe
            bot_tf = tr_robot_tf[0:1]
            bot_tf_num = int(tr_robot_tf[1:len(tr_robot_tf)])

            # Evaluate time and timeframe
            if (bot_tf.upper() == 'M' and (bar_minute + base_tf_num) % bot_tf_num == 0) \
                    or (bot_tf.upper() == 'H' and (bar_minute + base_tf_num == 0 or bar_minute + base_tf_num == 60)
                                and (bar_hour + 1) % bot_tf_num == 0):
                result = True

            if self.config['run_mode'] in ['debug', 'backtest']:
                dt = datetime.datetime.now().astimezone(utc_tz)
                log.debug(time="{}".format(dt.isoformat())
                     , level="DEBUG", events="Check trade time"
                     , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name
                     , acc_number="{}".format(self.acc_number)
                     , details="Check successful")
        except Exception as e:
            raise error.StrategyRobotError('Check trade time error: {}'.format(e))
        return result

    def load_trading_robots(self):
        trading_robots = {}
        conf_trading_robots = self.config['trading_robots']
        try:
            if conf_trading_robots is not None and len(conf_trading_robots) > 0:
                # Load list of model config and convert to dictionary, key is model name
                conf_models_dict = {}
                conf_models = None
                if 'models' in self.config:
                    conf_models = self.config['models']
                if conf_models is not None and len(conf_models) > 0:
                    for conf_model in conf_models:
                        conf_models_dict[conf_model['name']] = conf_model

                tr_bot_names = []
                # Loop through all trading robots under this strategy robot
                for tr_bot_conf in conf_trading_robots:
                    robot_name = tr_bot_conf['name']
                    tr_bot_names.append(robot_name)

                    # 1) Load config related to each trading robot
                    #tr_bot_config = self.load_trading_robot_config(robot_name)

                    # 1) Create robot context object for each trading robot using dynamic import
                    class_tr_bot_ctx = getattr( import_module(self.config['root_module_path'] + '.robot_context')
                                          , 'RobotContext' )
                    tr_bot_ctx = class_tr_bot_ctx(self.config)

                    # 2) Create model repository using dictionary for each trading robot
                    tr_bot_ml_models= {}
                    tr_bot_conf_models = tr_bot_conf['model_names'] if 'model_names' in tr_bot_conf else None
                    if tr_bot_conf_models is not None and len(tr_bot_conf_models) > 0:
                        for model_name in tr_bot_conf_models:
                            conf_model = conf_models_dict[model_name]
                            # Ignore copy model if algorithm is 'RULE_BASE'
                            if conf_model['algorithm'] != 'RULE_BASE':
                                tr_bot_ml_models[model_name] = self.ml_models[model_name]
                    tr_bot_ctx.ml_models = tr_bot_ml_models
                    tr_bot_ctx.datasets = self.datasets

                    # 3) Create trading robot object for each trading robot using dynamic import
                    class_tr_robot = getattr(import_module(self.config['root_module_path'] + '.trading_robot')
                                        , 'TradingRobot')
                    trading_robots[robot_name] = class_tr_robot(robot_name, tr_bot_ctx, tr_bot_conf)

                if self.config['run_mode'] in ['debug', 'backtest']:
                    dt = datetime.datetime.now().astimezone(utc_tz)
                    log.debug(time="{}".format(dt.isoformat())
                         , level="DEBUG", events="Load trading robots"
                         , st_bot=self.strategy_name
                         , details='Load successful: {}'.format(tr_bot_names))
                # End for
            # End if
        except Exception as e:
            raise error.StrategyRobotError('Load trading robots error: {}'.format(e))
        return trading_robots

    def run_trading_robot(self, trading_robot, queue):
        result = trading_robot.run()
        queue.put(result)

    def pre_execute(self, exec_output, **kwargs):
        return exec_output

    def execute(self, exec_output, **kwargs):
        try:
            # ========================================================================================
            # ========================================================================================
            # 1) Start all trading robots using multi-processing
            if self.config['run_mode'] == 'live':
                queue = multiprocessing.SimpleQueue() # Use queue to store returned results
                processes = []
                for bot in list(self.trading_robots.values()):
                    process = multiprocessing.Process(target=self.run_trading_robot, args=(bot, queue,))
                    processes.append(process)

                for p in processes:
                    p.start()

                dt = datetime.datetime.now().astimezone(utc_tz)
                log.info(time="{}".format(dt.isoformat())
                         , level="INFO", events="Execute strategy"
                         , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name
                         , acc_number="{}".format(self.acc_number)
                         , details='Start all trading robot processes successful')
            # ========================================================================================
            # ========================================================================================
            # 2) Get predict results from trading robot processes, each output is a list of trading signals or actions
                for _ in processes:
                    tr_bot_result = queue.get() # Get returned results from queue

                    if tr_bot_result['predict_result'] is not None:
                        exec_output['predict_results'].append(tr_bot_result['predict_result'])

                    elif tr_bot_result['predict_result'] is not None:
                        exec_output['predict_errors'].append(tr_bot_result['predict_error'])

                dt = datetime.datetime.now().astimezone(utc_tz)
                log.info(time="{}".format(dt.isoformat())
                         , level="INFO", events="Execute strategy"
                         , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name
                         , acc_number="{}".format(self.acc_number)
                         , details='Get predict results from trading robot processes and build strategy execution output successful')
            # ========================================================================================
            # ========================================================================================
            elif self.config['run_mode'] == 'debug':
                for tr_robot in list(self.trading_robots.values()):
                    tr_robot.run() # use this line for debug

        except Exception as e:
            err_msg = 'Strategy execution error: {}'.format(e)
            dt = datetime.datetime.now().astimezone(utc_tz)
            log.info(time="{}".format(dt.isoformat())
                     , level="INFO", events="Execute strategy"
                     , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name
                     , acc_number="{}".format(self.acc_number)
                     , details=err_msg)
            raise error.StrategyRobotError(err_msg)

        return exec_output

    def post_execute(self, exec_output, **kwargs):
        return exec_output
