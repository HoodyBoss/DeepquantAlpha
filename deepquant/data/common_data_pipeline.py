from structlog import wrap_logger, PrintLogger
from structlog.processors import JSONRenderer

import deepquant.common.datetime_util as datetime_util
import deepquant.common.mlmodel_util as mlmodel_util
import deepquant.common.json_util as json_util
import deepquant.common.error as error

from deepquant.data.pipeline import DataPipeline

logger = wrap_logger(PrintLogger(), processors=[JSONRenderer()])
log = logger.bind(time="{}".format(datetime_util.utcnow().isoformat())
                  , level="INFO", events="NONE", correl_id="NONE"
                  , st_bot="NONE", details="NONE")


class CommonDataPipeline(DataPipeline):

    def __init__(self, robot_config, symbol_prices, ml_models, correl_id=None):
        self.robot_config = robot_config
        self.symbol_prices = symbol_prices
        self.correl_id = correl_id

        self.market = robot_config['market']
        self.broker_id = robot_config['broker_id']
        self.data_path = robot_config['data_path']

        self.conn_config = { 'db_host' : robot_config['database_host'], 'db_port' : robot_config['database_port'] }

        self.price_to_process = {}
        self.feature_to_process = []

        self.datasets = {}
        self.ml_models = ml_models

        try:
            dataset_config = {}
            models_config = robot_config['models']
            if models_config is not None:
                for model_conf in models_config:
                    model_name = model_conf['name']
                    algorithm = model_conf['algorithm']
                    bar_num_require = model_conf['bar_num_require']
                    if algorithm.upper() == 'RULE_BASE':
                        dataset_config[model_name] = { 'model_name':model_name, 'algorithm':algorithm, 'bar_num_require':bar_num_require }
                    else:
                        buildfeat_module = model_conf['buildfeat_module']
                        feature_file_path = '{}/{}.csv'.format(self.data_path, model_name)
                        dataset_config[model_name] = { 'model_name':model_name \
                                                        , 'algorithm':algorithm \
                                                        , 'buildfeat_module':buildfeat_module \
                                                        , 'feature_file_path':feature_file_path \
                                                        , 'bar_num_require':bar_num_require }

            trading_robots = robot_config['trading_robots']
            if trading_robots is not None and len(trading_robots) > 0:
                for tr_bot in trading_robots:
                    symbol_id = tr_bot['symbol']
                    symbol_name = self.get_symbol_name(symbol_id)
                    timeframe = tr_bot['timeframe']
                    model_names_conf = tr_bot['model_names']
                    for model_name in model_names_conf:
                        model_conf = self.get_model_config(model_name)
                        symbol_tf = '{}_{}'.format(symbol_name, timeframe)
                        self.price_to_process[symbol_tf] = {'symbol':symbol_name, 'timeframe':timeframe \
                                                  , 'bar_num_require':dataset_config[model_name]['bar_num_require']}

                        if model_conf['algorithm'] != 'RULE_BASE':
                            dataset_config[model_name]['symbol'] = symbol_name
                            dataset_config[model_name]['timeframe'] = timeframe
                            self.feature_to_process.append(dataset_config[model_name])
        except Exception as e:
            log.error(level="ERROR", events='Initialize data pipeline', correl_id='{}'.format(self.correl_id)
                      , st_bot=self.robot_config['strategy_name']
                      , details='Initialize CommonDataPipeline error: {}'.format(e))

        log.info(events='Initialize data pipeline', correl_id='{}'.format(self.correl_id)
                 , st_bot=self.robot_config['strategy_name']
                 , details='Initialize CommonDataPipeline successful')

        super().__init__(self.market , self.broker_id , self.conn_config)

    def get_symbol_name(self, symbol_id):
        symbol_name = None
        try:
            for symbol in self.robot_config['symbols']:
                if symbol['id'] == symbol_id:
                    symbol_name = symbol['name']
                    break
        except Exception as e:
            raise Exception('Get symbol name error: {}'.format(e))
        return symbol_name

    def get_model_config(self, model_name):
        result = None
        try:
            models_config = self.robot_config['models']
            if models_config is not None:
                for model_conf in models_config:
                    if model_conf['name'] == model_name:
                        result = model_conf
                        break
        except Exception as e:
            raise Exception('Get model configuration error: {}'.format(e))
        return result

    def load_ml_models(self):
        try:
            conf_models = None
            ml_model_names = []
            if 'models' in self.robot_config:
                conf_models = self.robot_config['models']
            if conf_models is not None and len(conf_models) > 0:
                root_path = self.robot_config['data_path'] + 'models'
                for conf_model in conf_models:
                    if conf_model['algorithm'].upper() != "RULE_BASE":
                        # Validate existing model, load model from file if does not exist
                        if conf_model['name'] not in self.ml_models.keys() or self.ml_models[conf_model['name']] == None:
                            ml_model_names.append(conf_model['name'])
                            ml_model = None
                            if conf_model['algorithm'].upper() == "DNN":
                                ml_model = mlmodel_util.load_dnn_model(root_path + conf_model['files'][0]
                                                                               , root_path + conf_model['files'][1])
                            elif conf_model['algorithm'].upper() == 'XGBOOST':
                                ml_model = mlmodel_util.load_xgboost_model(root_path + conf_model['files'][0])

                            # Ignore algorithm = RULE_BASE because it is not a ML model
                            # The rule based model object will be created in trade_model.py
                            self.ml_models[conf_model['name']] = ml_model

                log.info(events='Load ML models', correl_id='{}'.format(self.correl_id)
                         , st_bot=self.robot_config['strategy_name']
                         , details='CommonDataPipeline loaded ML models successful: {}'.format(ml_model_names))

        except Exception as e:
            raise error.DataProcessingError('Load ML models error: {}'.format(e))


    def start_flow(self):
        price_df_dict = {}
        try:
            if self.price_to_process is not None:
                for conf in list(self.price_to_process.values()):
                    symbol_tf = '{}_{}'.format(conf['symbol'], conf['timeframe'])
                    if symbol_tf in self.symbol_prices:
                        symbol_tf_key = symbol_tf
                    else:
                        symbol_tf_key = '{}|{}'.format(conf['symbol'], conf['timeframe'])
                    price_json = json_util.encode(self.symbol_prices[symbol_tf_key])
                    self.insert_price_to_db(conf['symbol'], conf['timeframe'], price_json)
                    price_df = self.load_price_from_db(conf['symbol'], conf['timeframe'] \
                                                           , upper_col_name=True, limit_rows=conf['bar_num_require'])
                    price_df_dict[symbol_tf] = price_df

        except Exception as e:
            log.error(level="ERROR", events='Start flow of data pipeline', correl_id='{}'.format(self.correl_id)
                      , st_bot=self.robot_config['strategy_name']
                     , details="CommonDataPipeline processed price error: {}".format(e))
            raise Exception("CommonDataPipeline processed price error: {}".format(e))

        try:
            trading_robots = self.robot_config['trading_robots']
            if trading_robots is not None and len(trading_robots) > 0:
                for tr_bot in trading_robots:
                    symbol_id = tr_bot['symbol']
                    symbol_name = self.get_symbol_name(symbol_id)
                    timeframe = tr_bot['timeframe']

                    model_names_conf = tr_bot['model_names']
                    for model_name in model_names_conf:
                        symbol_tf = '{}_{}'.format(symbol_name, timeframe)
                        self.datasets[model_name] = price_df_dict[symbol_tf]
        except Exception as e:
            log.error(level="ERROR", events='Start flow of data pipeline', correl_id='{}'.format(self.correl_id)
                      , st_bot=self.robot_config['strategy_name']
                      , details="CommonDataPipeline prepared price dataset error: {}".format(e))
            raise Exception("CommonDataPipeline prepared price dataset error: {}".format(e))

        try:
            if self.feature_to_process is not None and len(self.feature_to_process) > 0:
                for conf in self.feature_to_process:
                    symbol_tf = '{}_{}'.format(conf['symbol'], conf['timeframe'])
                    price_df = price_df_dict[symbol_tf]
                    feature_df = self.build_features(conf['buildfeat_module'], conf['feature_file_path'], price_df=price_df)
                    self.datasets[conf['model_name']] = feature_df

            self.load_ml_models()
        except Exception as e:
            log.error(level="ERROR", events='Start flow of data pipeline', correl_id='{}'.format(self.correl_id)
                      , st_bot=self.robot_config['strategy_name']
                      , details="CommonDataPipeline prepared features dataset error: {}".format(e))
            raise Exception("CommonDataPipeline prepared features dataset error: {}".format(e))