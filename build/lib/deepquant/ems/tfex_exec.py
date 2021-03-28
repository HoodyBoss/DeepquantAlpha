import multiprocessing
import time
import os, sys, getopt
import yaml
import math
import random
import pandas as pd
from importlib import import_module

from ecpy.curves import Curve
from ecpy.keys import ECPrivateKey
from ecpy.ecdsa import ECDSA
import base64
import hashlib
import binascii

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

import deepquant.common.datetime_util as datetime_util
import deepquant.common.line_notify as line_notify
import deepquant.common.json_util as json_util
import deepquant.common.cache_proxy as cache_proxy
from deepquant.common.state_machine import StateMachine

import deepquant.data.db_gateway as db_gateway

import deepquant.ems.market_util_set as market_util
from deepquant.ems.base_exec import BaseExec
from deepquant.ems.error import ExecutionError


class TfexExec(BaseExec):

    def __init__(self, robot_config):
        super().__init__(robot_config=robot_config)

        # ก่อนรันต้องสร้าง environment variable ชื่อ GOOGLE_APPLICATION_CREDENTIALS ใน OS ก่อน
        # โดยระบุค่าคือ file path ของไฟล์ credential นามสกุล .json เป็นไฟล์ที่สร้างในเว็บ Google
        # ขั้นตอน
        # 1. เข้าเว็บ https://console.firebase.google.com แล้วเลือก Firebase Project ที่สร้างไว้ ถ้ายังไม่ได้สร้างให้สร้าง Project ใหม่
        # 2. เลือก Settings -> Service accounts
        # 3. คลิ้ก Generate new private key แล้วยืนยันโดยกด Generate Key
        # 4. save ไฟล์ xxx.json นี้ไว้ในเครื่องในที่ที่ปลอดภัย และห้ามลืมล่ะ
        # 5. สร้าง environment variable ในเครื่อง ตั้งชื่อว่า GOOGLE_APPLICATION_CREDENTIALS ระบุ path พร้อมชื่อไฟล์ในข้อ 4
        # NOTE: Windows, Linux, Mac OS X สร้าง environment variable ไม่เหมือนกัน ให้ปรึกษาวิธีจากเพื่อนๆ ในกลุ่มนะครับ
        google_app_cred_env_var = 'GOOGLE_APPLICATION_CREDENTIALS'
        self.google_app_cred = os.environ[google_app_cred_env_var].strip()
        self.watch_collection = u'tfex_live_data'
        self.histdata_collection = u'tfex_hist_data'

        self.set_holidays = None # List of holidays (string)
        self.set_holidays_last_load_day = None # Format is %Y%m%d in int เช่น 20200502
        self.set_holidays_load_interval_day = 3 # Load holidays every 3 days

        self.price_decimal_digit = 2

        # ฟีลด์สำหรับเก็บค่าหลัง login ผ่านแล้ว
        # key คือ account number
        # value คือ dict {bearer, broker_id, access_token, refresh_token, expires_in, authenticated_userid, last_login_timestamp}
        # last_login_timestamp ใช้ค่า UTC timestamp ในหน่วย millisecond
        self.auth_accounts = {}
        self.login_url = 'https://open-api.settrade.com/api/oam/v1/{brokerId}/broker-apps/ALGO/login'
        self.refresh_token_url = 'https://open-api-test.settrade.com/api/oam/v1/{brokerId}/broker-apps/ALGO/refresh-token'

        self.init_robot_positions()
        print('Initialize TFEX execution finished')

        # FOR TESTING ONLY
        self.latest_prices = {'S50|M5': {'datetime':'20200605160000',
                                         'open':940.1,
                                         'high':941.8,
                                         'low':939.4,
                                         'close':941.7,
                                         'volume':555}}


    # Create a callback on_snapshot function to capture changes in google firestore
    def on_snapshot(self, doc_snapshot, changes, read_time):
        if self.is_market_open():
            for doc in doc_snapshot:
                candles = doc.to_dict()
                for symbol_tf in list(candles.keys()):
                    candle = candles[symbol_tf]
                    self.latest_prices[symbol_tf] = candle


    def start(self):
        cred = credentials.Certificate(self.google_app_cred)
        firebase_admin.initialize_app(cred)
        firestore_db = firestore.client()
        doc_ref = firestore_db.collection(self.watch_collection)
        # Watch the document on google firestore
        doc_ref.on_snapshot(self.on_snapshot)

        while True:
            if (self.robot_config['enable_check_trade_time'] == True and self.is_trade_time(check_last_bar=True))\
                    or self.robot_config['enable_check_trade_time'] == False:
                # Get time minute
                minute = int(datetime_util.local_now().strftime('%M'))

                """
                Design rationale:
                1. ใช้ % (mod) หากมีเศษนาทีแสดงว่าเลยช่วงจบแท่งและขึ้นแท่งใหม่มาแล้วอย่างน้อย 1 นาที ดังนั้นจึงอนุญาตให้ execute SL/TP ได้
                2. เก็บราคาล่าสุดที่เพิ่งประมวลผลเสร็จ เพื่อนมาตรวจสอบกับราคาล่าสุดที่เพิ่งรับหรือโหลดมาใหม่ หาก datetime, OHLCV ตรงกันหมด
                   ก็จะไม่ประมวลผล จะยกเว้นไปเลย เพื่อรองรับกรณี เช่น ตลาดปิด, ระบบ data feed มีปัญหา ฯ
                3. ทุกครั้งที่ประมวลผลแบบเป็นรอบ (interval) เมื่อจบแท่งแล้วขึ้นแท่งใหม่ จะ generate correlation ID ใหม่เสมอ
                   และเมื่อประมวลผลเสร็จก็จะจำค่าไว้ เพื่อนำมาเทียบกับค่าใหม่
                """
                """
                if minute % self.base_bar_interval > 0:
                    if self.latest_prices != self.prev_exec_sltp_prices:
                        # 1) Execute in-bar
                        self.execute_sltp()
                        # 2) Memorize latest executed prices
                        self.prev_exec_sltp_prices = self.latest_prices
                else:
                """
                if True:
                    # 1) Generate new correlation ID
                    correl_id = self.gen_correl_id()
                    if self.prev_correl_id != correl_id:
                    #if self.prev_correl_id != correl_id:
                    #    and self.latest_prices != self.prev_exec_prices:
                        self.cur_correl_id = correl_id

                        # 2) Backfill automatically
                        #if self.require_backfill():
                        #    self.backfill_price()

                        # 3) Execute when new bar started
                        self.execute(correl_id)
                        # 4) Memorize latest correlation ID and latest executed prices
                        self.prev_correl_id = self.cur_correl_id
                        self.prev_exec_prices = self.latest_prices

            # Take a rest ^^
            time.sleep(self.run_interval_sleep)

    # ==================================================================================================
    # BEGIN: Coarse-grained execution helper methods
    # ==================================================================================================
    def build_trade_input(self, **kwargs):
        """
        Override method
        """
        correl_id = kwargs['correl_id']
        server_time = kwargs['server_time']
        local_time = kwargs['local_time']

        # 1) Load price
        # Format of symbol_prices is dictionary of JSON. Dictionary key is <SYMBOL|TF>
        symbol_prices = self._get_symbol_prices(self.symbol_tf_list)
        # Uses datetime of first trading robot as base_time. Format is %Y%m%d%H%M%S เช่น 20200824171500
        self.base_time = str(symbol_prices[list(symbol_prices.keys())[0]]['datetime'])

        # 2) Parallelize the data loading, 1 process / 1 trading account, 1 trading robot / 1 symbol
        if self.robot_config['run_mode'] == 'live':
            queue = multiprocessing.SimpleQueue()  # Use queue to store returned results
            processes = []
            for account_conf in self.robot_config['accounts']:
                process = multiprocessing.Process(target=self.__prepare_account_data_task
                                                  , args=(correl_id
                                                          , server_time
                                                          , local_time
                                                          , account_conf['account_number']
                                                          , queue,))
                processes.append(process)

            for p in processes:
                p.start()
            """
            dt = datetime.datetime.now().astimezone(utc_tz)
            log.info(time="{}".format(dt.isoformat())
                     , level="INFO", event="Execute strategy"
                     , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name
                     , acc_number="{}".format(self.acc_number)
                     , details='Start all trading robot processes successful')
            """
            # ========================================================================================
            # ========================================================================================
            # NOTE: 1 account / 1 trading robot / 1 list element
            data_list = []
            for _ in processes:
                data = queue.get()  # Get returned results from queue
                data_list.append(data)
            """
            dt = datetime.datetime.now().astimezone(utc_tz)
            log.info(time="{}".format(dt.isoformat())
                     , level="INFO", event="Execute strategy"
                     , correl_id="{}".format(self.correl_id), st_bot=self.strategy_name
                     , acc_number="{}".format(self.acc_number)
                     , details='Get predict results from trading robot processes and build strategy execution output successful')
            """
        # ========================================================================================
        # ========================================================================================
        elif self.robot_config['run_mode'] == 'debug':
            for account_conf in self.robot_config['accounts']:
                data_list = []
                data = self.__prepare_account_data_task(correl_id, server_time, local_time, account_conf['account_number'])
                data_list.append(data)

        # 3) Call data pipeline to start flow of dataset preparation
        # The 'ml_models' performs like a ML models cache
        class_pipeline = getattr(import_module(self.robot_config['data_pipeline_module_path']
                                               , self.robot_config['data_pipeline_class']))
        pipeline = class_pipeline(self.robot_config, symbol_prices, self.ml_models, correl_id=correl_id)
        pipeline.start_flow()
        datasets = pipeline.datasets

        # 4) Set trading input
        trade_input = { 'correl_id' : correl_id
                        , 'strategy_name': self.robot_config['strategy_name']
                        , 'prices' : symbol_prices
                        , 'datasets': datasets
                        , 'trade_data' : data_list }

        return trade_input


    def handle_predictions(self, **kwargs):
        trade_actions = None

        trade_input = kwargs['trade_input']

        # Create strategy endpoint object
        class_endpoint = getattr(import_module(self.robot_config['root_module_path'] + '.endpoint')
                                 , 'StrategyRobotEndpoint')
        endpoint = class_endpoint(self.robot_config
                                  , trade_input['correl_id']
                                  , trade_input['trade_data'][0]['base_time']
                                  , trade_input['timestamp']
                                  , trade_input
                                  , self.ml_models)

        try:
            # Call main strategy endpoint's method
            exec_output = endpoint.execute_strategy()
            if len(exec_output['predict_errors']) > 0:
                for msg_idx in range(0, len(exec_output['predict_errors'])):
                    err_msg = str(exec_output['predict_errors'][msg_idx])
                    line_notify.send_notify(self.robot_config['strategy_name']
                                            , self.robot_config['admin_notify_token']
                                            , err_msg)

            if 'predict_result' in exec_output:
                trade_actions = exec_output['predict_results']

        except:
            exec_output = 'Failed strategy execution'
            line_notify.send_notify(self.robot_config['strategy_name']
                                    , self.robot_config['admin_notify_token']
                                    , exec_output)

        return trade_actions


    def handle_trade_actions(self, **kwargs):
        trade_input = kwargs['trade_input']
        trade_actions = kwargs['trade_actions']

        if trade_actions is not None and len(trade_actions) > 0:
            # Parallelize the trading action handling, 1 process / 1 trading robot
            processes = []
            for action in trade_actions:
                account_index, trading_robot_index = self.__get_trade_data_index(trade_input['trade_data']
                                                                               , action['robot_name'])
                account_info = trade_input[account_index]['account_info']
                account_config = trade_input[account_index]['account_config']
                trade_data = trade_input[account_index]['trading_robots'][trading_robot_index]
                process = multiprocessing.Process(target=self.__handle_action_task, args=(action
                                                                                         , trade_input
                                                                                         , account_info
                                                                                         , account_config
                                                                                         , trade_data,))
                processes.append(process)

            for p in processes:
                p.start()

        return trade_input['correl_id']
    # ==================================================================================================
    # END: Coarse-grained execution helper methods
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Task methods
    # ==================================================================================================
    def __prepare_account_data_task(self, correl_id, server_time, local_time, account_number, queue=None, **kwargs):
        # Load account
        account_conf = self.__get_account_config(account_number)

        # Prepare authorization token
        base_token = open(os.environ[account_conf['base_token_env_var']]).read().strip()
        http_header = {'Authorization': 'Bearer {}'.format(base_token)
            , 'Content-Type': 'application/json'}

        self._prepare_auth(account_conf)




        # Load account info, portfolio, positions, orders
        account_info = self.get_account_info(account_conf=account_conf, http_header=http_header)
        # portfolio = self.get_portfolio(account_conf=account_conf, http_header=http_header)
        positions = self.get_positions(account_conf=account_conf, http_header=http_header, map_to_internal_position=True)
        orders = self.get_orders(account_conf=account_conf, http_header=http_header)

        # Load trading robot configurations
        trading_robot_config_list = self.__get_trading_robot_configs(account_info['accountNo'])

        data = {
            'correl_id': correl_id
            , 'strategy_name': self.robot_config['strategy_name']
            , 'base_time': self.base_time
            , 'server_time': server_time
            , 'local_time': local_time
            , 'account_info': account_info
            , 'account_config': account_conf
            , 'http_header' : http_header
        }

        # Set specific data for each trading robots under this account
        tr_robots = []
        for tr_robot_config in trading_robot_config_list:
            tr_robot = { 'robot_name' : tr_robot_config['name']
                        , 'config': tr_robot_config
            }

            symbol = self.__get_market_symbol(tr_robot_config['symbol'])
            #tr_robot['portfolio'] = portfolio
            tr_robot['positions'] = self.__get_positions_by_symbol(positions, symbol)
            tr_robot['orders'] = self.__get_orders_by_symbol(orders, symbol)
            tr_robot['symbol'] = symbol

            tr_robots.append(tr_robot)

        data['trading_robots'] = tr_robots

        if queue is not None:
            # Put data into queue
            queue.put(data)
        else:
            return data


    def __handle_action_task(self, trade_input, trade_action, account_info, account_config, trade_data):
        sm = StateMachine

        # Initialize cache proxy (a redis wrapper). One connection / trade action handling task
        cache = cache_proxy.CacheProxy(self.robot_config['cache_host'], self.robot_config['cache_port'])

        robot_name = trade_action['robot_name']
        action_code = trade_action['action_code']
        symbol_info = trade_input['symbol_infos'][robot_name]
        symbol_tick_size = symbol_info['tick_size']

        exec_params = { 'trade_action' : trade_action
                        , 'account_info' : account_info
                        , 'account_config' : account_config
                        , 'trade_input' : trade_input
                        , 'trade_data' : trade_data
                        , 'symbol_info': symbol_info
                        , 'cache': cache }

        symbol = trade_data['symbol']
        positions = exec_params['trade_data']['positions']
        orders = exec_params['trade_data']['orders']

        position = self.__get_active_position_by_symbol(positions, symbol)

        notify_msg = ''

        try:
            if action_code in [sm.ACTION_OPEN_BUY, sm.ACTION_OPEN_SELL, sm.ACTION_MODIFY_POSITION
                               , sm.ACTION_CLOSE_BUY, sm.AcTION_CLOSE_SELL
                               , sm.ACTION_SCALE_OUT_BUY, sm.ACTION_SCALE_OUT_SELL]:

                notify_msg = super()._handle_action_task_common(trade_action, position, exec_params)

            # ==========================================================================================
            elif action_code in [sm.ACTION_SCALE_IN_BUY, sm.ACTION_SCALE_IN_SELL]:
                try:
                    # 1) Open position
                    self.open_position(exec_params=exec_params)
                    # 2) Wait a seconds
                    time.sleep(self.wait_order_exec_seconds)
                    # 3) Get latest trade and then validate trading status
                    latest_position = self.__get_latest_position(self, exec_params, symbol_tick_size)
                    if latest_position is not None and position['quantity'] != latest_position['quantity']:
                        # Memorize stop loss and take profit
                        self.memorize_cache_robot_position(cache, trade_action['label'], trade_action['robot_name']
                                                           , trade_action['symbol_name'], trade_action['trade_type']
                                                           , latest_position['quantity']
                                                           , trade_action['stop_loss']
                                                           , trade_action['take_profit'])
                    else:
                        if latest_position is not None and position['quantity'] == latest_position['quantity']:
                            raise ExecutionError("Scale in position successful but the quantity has not been changed -> label:{}".format(
                                trade_action['label']))

                    notify_msg = ''
                except ExecutionError as e:
                    raise ExecutionError('Scale in fail -> {}'.format(e))

            # ==========================================================================================
            if notify_msg != '':
                # Send notification
                line_notify.send_notify(self.robot_config['strategy_name']
                                        , self.robot_config['admin_notify_token']
                                        , notify_msg)

        except ExecutionError as exec_err:
            # Send notification
            line_notify.send_notify(self.robot_config['strategy_name']
                                    , self.robot_config['admin_notify_token']
                                    , 'Execution fail. -> {}'.format(exec_err))
        finally:
            # Reset trading positions that has been closed
            self.reset_trading_state(exec_params=exec_params)
            # Save latest trading positions
            self.save_trading_state(exec_params=exec_params)
    # ==================================================================================================
    # END: Task methods
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Protected methods
    # ==================================================================================================
    def _get_symbol_prices(self, symbol_tf_list):
        """
        Returns dictionary of JSON, dictionary key is <SYMBOL|TF>
        """
        symbol_prices = {}

        if self.latest_prices is not None:
            for price_key in list(self.latest_prices.keys()):
                keys = price_key.split(self.symbol_tf_delimiter)
                symbol_from_feed = keys[0]
                tf_from_feed = keys[1]

                # วนลูปตรวจสอบ symbol กับ timeframe ว่าที่โหลดมามีตรงกับในลิสต์ <SYMBOL|TF> ที่จะเทรดหรือไม่
                # ถ้ามีก็จะเซ็ตค่าราคาล่าสุด
                found_match = False
                symbol_tf_index = 0
                while found_match == False:
                    symbol_tf = symbol_tf_list[symbol_tf_index]
                    s = symbol_tf.split(self.symbol_tf_delimiter)
                    symbol = s[0]
                    tf = s[1]

                    if symbol_from_feed == symbol and tf_from_feed == tf:
                        symbol_prices[symbol_tf] = self.latest_prices[price_key]
                        found_match = True

                    symbol_tf_index = symbol_tf_index + 1

        return symbol_prices


    def _get_active_position(self, positions, label=None, symbol=None, **kwargs):
        return self.__get_active_position_by_symbol(positions, symbol)


    def _map_account(self, account_number, original_account, **kwargs):
        account_info = {}
        settrade_account = original_account
        account_info['acc_number'] = account_number
        account_info['balance'] = float(settrade_account['cashBalance'])
        account_info['call_force_flag'] = float(settrade_account['callForceFlag'])
        account_info['call_force_margin'] = float(settrade_account['callForceMargin'])
        account_info['credit_line'] = float(settrade_account['creditLine'])
        account_info['equity'] = float(settrade_account['equity'])
        account_info['excess_equity'] = float(settrade_account['excessEquity'])
        account_info['total_mm'] = float(settrade_account['totalMM'])
        account_info['total_mr'] = float(settrade_account['totalMR'])
        return account_info


    def _map_position(self, original_position, symbol_tick_size, **kwargs):
        position = None
        settrade_position = original_position
        robot_name = kwargs['robot_name']

        label = self.robot_labels[robot_name]

        # Load position from cache
        if 'cache' in kwargs and kwargs['cache'] is not None:
            cache = kwargs['cache']
        else:
            cache = cache_proxy.CacheProxy(self.robot_config['cache_host'], self.robot_config['cache_port'])
        pos_from_cache = cache.get('pos_{}'.format(label))

        if pos_from_cache is not None and pos_from_cache['robot_name'] == robot_name \
                and settrade_position['symbol'] == pos_from_cache['symbol_name']:
            position = {}

            position['label'] = label
            position['symbol_name'] = settrade_position['symbol']
            position['underlying'] = settrade_position['underlying']
            position['market_price'] = float(settrade_position['marketPrice'])

            trade_type = position['hasLongPosition']
            if trade_type == 'LONG':
                position['trade_type'] = 'BUY'
                position['quantity'] = int(settrade_position['actualLongPosition'])
                position['available_quantity'] = int(settrade_position['availableLongPosition'])
                position['avg_cost'] = float(settrade_position['longAvgCost'])
                position['avg_price'] = float(settrade_position['longAvgPrice'])
                position['entry_price'] = float(settrade_position['startLongPrice'])
                position['entry_cost'] = float(settrade_position['startLongCost'])
                position['entry_quantity'] = float(settrade_position['startLongPosition'])
                position['profit_points'] = position['market_price'] - position['avg_price']

            elif trade_type == 'SHORT':
                position['trade_type'] = 'SELL'
                position['quantity'] = int(settrade_position['actualShortPosition'])
                position['available_quantity'] = int(settrade_position['availableShortPosition'])
                position['avg_cost'] = float(settrade_position['shortAvgCost'])
                position['avg_price'] = float(settrade_position['shortAvgPrice'])
                position['entry_price'] = float(settrade_position['startShortPrice'])
                position['entry_cost'] = float(settrade_position['startShortCost'])
                position['entry_quantity'] = float(settrade_position['startShortPosition'])
                position['profit_points'] = position['avg_price'] - position['market_price']

            position['gross_profit'] = 0.0
            position['net_profit'] = float(settrade_position['realizedPL'])
            position['trans_cost'] = 0.0

            stop_loss = pos_from_cache['stop_loss']
            take_profit = pos_from_cache['take_profit']

            avg_price = position['avg_price']

            if position['trade_type'] == 'BUY':
                if stop_loss > 0:
                    position['stop_loss_points'] = abs(avg_price - stop_loss)
                else:
                    position['stop_loss_points'] = 0.0

                if take_profit > 0:
                    position['take_profit_points'] = abs(take_profit - avg_price)
                else:
                    position['take_profit_points'] = 0.0

            elif position['trade_type'] == 'SELL':
                if stop_loss > 0:
                    position['stop_loss_points'] = abs(stop_loss - avg_price)
                else:
                    position['stop_loss_points'] = 0.0

                if take_profit > 0:
                    position['take_profit_points'] = abs(avg_price - take_profit)
                else:
                    position['take_profit_points'] = 0.0

        return position


    def _get_latest_position(self, exec_params, symbol_tick_size, **kwargs):
        symbol = exec_params['symbol_info']['name']
        positions = self.get_positions(exec_params=exec_params, symbol=symbol, map_to_internal_position=True)
        position = self.__get_active_position_by_symbol(self, positions, symbol)
        return position

    def _sign(self, api_key, api_secret, params):
        cv = Curve.get_curve('secp256r1')

        dt = datetime_util.utcnow()
        timestamp = int(dt.timestamp() * 1000)

        payload = "{}.{}.{}".format(api_key, params, timestamp)
        hashed_payload = hashlib.sha256(payload.encode("UTF-8")).hexdigest()

        pv_key = ECPrivateKey(
            int(binascii.hexlify(base64.b64decode(api_secret)), 16), cv)
        signature_bytes = ECDSA().sign(bytearray.fromhex(hashed_payload), pv_key)
        return binascii.hexlify(signature_bytes).decode("UTF-8"), timestamp


    def _login(self, account_conf):
        # Load signature
        #base_token = json_util.load(os.environ[account_conf['base_token_env_var']])
        base_token = json_util.load('/Users/minimalist/settrade_open_api_tk/tk_caf1.dq')

        app_id = base_token['app_id']
        app_secret = base_token['app_secret']

        login_signature, login_timestamp = self._sign(app_id, app_secret, "")

        api_key = app_id

        # Login
        retry = 0
        login_result = False
        while login_result != True and retry < self.max_retry:
            json_payload = {'apiKey': api_key, 'params': '', 'signature': login_signature, 'timestamp': login_timestamp}
            response = self.send_http_post_request(self.login_url.replace('{brokerId}', str(account_conf['broker_id']))
                                                   , json_payload
                                                   , header_dict=self.http_header)
            if response.status_code == 200:
                response_dict = json_util.decode(response.text)
                response_dict['last_login_timestamp'] = login_timestamp
                self.auth_accounts[account_conf['account_number']] = response_dict
                login_result = True
            else:
                time.sleep(self.wait_seconds)
                retry = retry + 1

        if login_result == False:
            raise ExecutionError("Log in failed for account '{}'".format(account_conf['account_number']))


    def _refresh_token(self, account_conf):
        # Load signature
        # base_token = json_util.load(os.environ[account_conf['base_token_env_var']])
        base_token = json_util.load('/Users/minimalist/settrade_open_api_tk/tk_caf1.dq')
        app_id = base_token['app_id']
        api_key = app_id

        # Refresh token
        retry = 0
        refresh_result = False
        while refresh_result != True and retry < self.max_retry:
            refresh_token = self.auth_accounts[account_conf['account_number']]['refresh_token']
            json_payload = {'apiKey': api_key, 'refreshToken': refresh_token}
            response = self.send_http_post_request(self.refresh_token_url.replace('{brokerId}', account_conf['broker_id'])
                                                   , json_payload
                                                   , header_dict=self.http_header)
            if response.status_code == 200:
                response_dict = json_util.decode(response.text)
                self.auth_accounts[account_conf['account_number']]['access_token'] = response_dict['access_token']
                refresh_result = True
            else:
                time.sleep(self.wait_seconds)
                retry = retry + 1

        if refresh_result == False:
            raise ExecutionError("Refresh token failed for account '{}'".format(account_conf['account_number']))


    def _prepare_auth(self, account_conf):
        account_number = account_conf['account_number']
        if account_number in list(self.auth_accounts.keys() and self.auth_accounts[account_number] is not None):
            auth_acc = self.auth_accounts[account_number]
            last_login_timestamp = auth_acc['last_login_timestamp']
            expires_in = auth_acc['expires_in'] * 1000 # convert millisecond to second
            cur_timestamp = int(datetime_util.utcnow().timestamp() * 1000)

            if cur_timestamp - last_login_timestamp >= int(expires_in / 3):
                # Refresh token
                self._refresh_token(account_conf)
        else:
            # Login
            self._login(account_conf)
    # ==================================================================================================
    # END: Protected methods
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Private methods
    # ==================================================================================================
    def __build_random_pos_size_list(self, pos_size):
        # พิจารณาจากสภาพคล่อง เช่น ไม่เกิน 25 สัญญาต่อออร์เดอร์
        pos_size_liquid_limit = self.pos_size_liquid_limit
        # เช่น 0.25 -> 25%
        init_reduce_percent = self.init_reduce_pos_size_percent
        pos_size_list = []

        if pos_size <= pos_size_liquid_limit:
            pos_size_list.append(pos_size)
        else:
            remain_per_set = 0
            pos_size_set = math.ceil(round(pos_size / pos_size_liquid_limit, 2))
            last_set_remain = pos_size % pos_size_liquid_limit

            for i in range(pos_size_set):
                if i == pos_size_set - 1 and last_set_remain > 0:
                    pos_size_liquid_limit = last_set_remain

                reduce_size = int(random.random() * (pos_size_liquid_limit * init_reduce_percent))
                random_pos_size = pos_size_liquid_limit - reduce_size
                pos_size_list.append(random_pos_size)
                remain_per_set = remain_per_set + (pos_size_liquid_limit - random_pos_size)

            if remain_per_set > 0:
                pos_size_list.append(remain_per_set)

        return pos_size_list


    def __get_account_config(self, account_number):
        """
        Get account configuration under account_number
        """
        account_conf = None
        for conf in self.robot_config['accounts']:
            if conf['account_number'] == account_number:
                account_conf = conf
                break
        return account_conf


    def __get_trading_robot_configs(self, account_number):
        """
        Get list of trading robot configurations under account_number
        """
        tr_robot_config_list = []
        for conf in self.robot_config['trading_robots']:
            if conf['account_number'] == account_number:
                tr_robot_config_list.append(conf)
        return tr_robot_config_list


    def __get_trade_data_index(self, trade_data_list, trading_robot_name):
        account_index = -1
        trading_robot_index = -1

        for i in range(0, len(trade_data_list)):
            trading_robot_list = trade_data_list[i]['trading_robots']
            for j in range(0, len(trading_robot_list)):
                if trading_robot_list[j]['robot_name'] == trading_robot_name:
                    account_index = i
                    trading_robot_index = j
                    break
        return account_index, trading_robot_index


    def __is_active_order(self, order_status):
        if order_status.lower() in ['pending', 'queuing']:
            return True
        else:
            return False


    def __has_active_orders_by_symbol(self, orders, symbol):
        result = False
        if orders is not None:
            for order in orders:
                if order['symbol'] == symbol and self.__is_active_order(order['status']) == True:
                    result = True
        return result


    def __get_orders_by_symbol(self, orders, symbol):
        symbol_orders = []

        if orders is not None:
            for order in orders:
                if order['symbol'] == symbol:
                    symbol_orders.append(order)

        return symbol_orders


    def __get_positions_by_symbol(self, positions, symbol):
        symbol_positions = []

        if positions is not None:
            for pos in positions:
                if pos['symbol'] == symbol:
                    symbol_positions.append(pos)

        return symbol_positions


    def __get_active_position_by_symbol(self, positions, symbol):
        position = None

        if positions is not None:
            for pos in positions:
                if pos['symbol'] == symbol and pos['quantity'] > 0 and pos['symbol_name'] == symbol:
                    position = pos
                    break

        return position


    def __create_close_order_req(self, account_conf, trade_action):
        sm = StateMachine

        if trade_action['action_code'] in [sm.ACTION_CLOSE_BUY, sm.ACTION_SCALE_OUT_BUY]:
            side = 'SHORT'
        elif trade_action['action_code'] in [sm.ACTION_CLOSE_SELL, sm.ACTION_SCALE_OUT_SELL]:
            side = 'LONG'

        if 'price_type' in trade_action and trade_action['price_type'] in ['LIMIT', 'ATO', 'MP', 'MP-MTL', 'MP-MKT']:
            price_type = trade_action['price_type']
        else:
            raise Exception('Create close order request payload fail -> price type in trade action is invalid')

        if 'symbol' in trade_action:
            symbol = trade_action['symbol_name']
        else:
            raise Exception('Create close order request payload fail -> symbol in trade action is invalid')

        if 'action_price' in trade_action:
            action_price = trade_action['action_price']
        else:
            action_price = 0.0

        if 'volume' in trade_action:
            volume = trade_action['volume']
        else:
            raise Exception('Create close order request payload fail -> volume in trade action is invalid')

        request_payload = {
                            "pin": account_conf['pin'],
                            "position": "CLOSE",
                            "price": round(action_price, self.price_decimal_digit),
                            "priceType": price_type,
                            "side": side,
                            "symbol": symbol,
                            "volume": int(volume)
                            }
        return request_payload


    def __create_open_order_req(self, account_conf, trade_action):
        sm = StateMachine

        if trade_action['action_code'] in [sm.ACTION_OPEN_BUY, sm.ACTION_SCALE_IN_BUY]:
            side = 'LONG'
        elif trade_action['action_code'] in [sm.ACTION_OPEN_SELL, sm.ACTION_SCALE_IN_SELL]:
            side = 'SHORT'

        if 'price_type' in trade_action and trade_action['price_type'] in ['LIMIT', 'ATO', 'MP', 'MP-MTL', 'MP-MKT']:
            price_type = trade_action['price_type']
        else:
            raise Exception('Create open order request payload fail -> price type in trade action is invalid')

        if 'symbol' in trade_action:
            symbol = trade_action['symbol_name']
        else:
            raise Exception('Create open order request payload fail -> symbol in trade action is invalid')

        if 'action_price' in trade_action:
            action_price = trade_action['action_price']
        else:
            action_price = 0.0

        if 'volume' in trade_action:
            volume = trade_action['volume']
        else:
            raise Exception('Create open order request payload fail -> volume in trade action is invalid')

        request_payload = {
            "pin": account_conf['pin'],
            "position": "OPEN",
            "price": round(action_price, self.price_decimal_digit),
            "priceType": price_type,
            "side": side,
            "symbol": symbol,
            "volume": int(volume)
        }
        return request_payload


    def __place_order(self, order_type, **kwargs):
        exec_params = kwargs['exec_params']
        account_conf = exec_params['trade_data']['account_conf']
        http_header = exec_params['trade_data']['http_header']
        trade_action = exec_params['trade_action']
        symbol = exec_params['trade_data']['symbol']

        result = False

        retry = 0
        success = False
        while success != True and retry < self.max_retry:
            if order_type == 'CLOSE':
                payload_dict = self.__create_close_order_req(account_conf, trade_action)
            elif order_type == 'OPEN':
                payload_dict = self.__create_open_order_req(account_conf, trade_action)

            payload_json = json_util.encode(payload_dict)

            # Place order
            # หาก open position แล้ว fail จะไม่ส่งซ้ำ ยกเลิกการเปิดสถานะไปเลย กันปัญหาจากการดีเลย์
            # แต่ถ้าเป็นการ close position หรือส่ง stop order หาก fail จะวนลูปเพื่อส่งซ้ำ
            order_no == 0
            if order_type in ['CLOSE', 'STOP'] or (order_type == 'OPEN' and retry == 0):
                url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/orders')
                url = url.format(account_conf['broker_id'], account_conf['account_number'])
                response = self.send_http_post_request(url, payload_json, header_dict=http_header)
                if response is not None and response.status_code == 200:
                    order_no = json_util.decode(response)['orderNo']

            # Wait
            time.sleep(self.wait_order_exec_seconds)

            # ==========================================================================================
            # Check sending the close or open action
            if order_type in ['CLOSE', 'OPEN']:
                # Get latest positions and then get active position for this symbol
                positions = self.get_positions(exec_params, symbol=symbol, map_to_internal_position=True)
                active_position = self.__get_active_position_by_symbol(self, positions, symbol)

                # Validate action result
                if (order_type == 'CLOSE' and active_position == None) \
                        or (order_type == 'OPEN' and active_position is not None):
                    success = True
                    result = True
                elif order_type == 'OPEN' and active_position is None:
                    # Wait again, รอนานขึ้นอีกนิด เผื่อดีเลย์ เช่น ค่อยๆ เกิด partial match
                    time.sleep(self.wait_order_exec_seconds * 5)

                    # Get latest positions and then get active position for this symbol
                    positions = self.get_positions(exec_params, symbol=symbol, map_to_internal_position=True)
                    active_position = self.__get_active_position_by_symbol(self, positions, symbol)

                    # ไม่ลอง open position ใหม่ ยกเลิกไปเลย แล้ว notify แจ้ง error แทน
                    # เพื่อป้องกันปัญหาจากการดีเลย์ของตลาดฯ หรือเน็ตเวิร์ก
                    # เช่น หากลอง open position ใหม่ แต่ออร์เดอร์ที่ส่งไปเมื่อสักครู่กำลัง match พอดี เพราะดีเลย์
                    # หากส่ง open order ไปใหม่จะกลายเป็นการ open position เพิ่ม
                    if order_type == 'OPEN' and active_position is None:
                        success = True
                        result = False
                else:
                    time.sleep(self.wait_seconds)
                    retry = retry + 1

        # ==============================================================================================
        if (order_type == 'CLOSE' and retry == self.max_retry and success == False) \
                or (order_type == 'OPEN' and success == True and result == False):
            raise Exception('Place order fail -> {}'.format(order_type.lower()))

        return result
    # ==================================================================================================
    # END: Private methods
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Fine-grained execution helper methods
    # ==================================================================================================
    def is_trade_time(self, **kwargs):
        bkk_now = datetime_util.bangkok_now()
        bkk_day = bkk_now.strftime('%Y%m%d')
        bkk_time = int(bkk_now.strftime('%H%M%S'))
        # Python: 0 = Monday, 4 = Friday
        weekday = bkk_now.weekday()

        # Load SET holidays
        if self.set_holidays == None \
                or int(bkk_day) - self.set_holidays_last_load_day >= self.set_holidays_load_interval_day:
            self.set_holidays = market_util.load_holidays()
            self.set_holidays_last_load_day = int(bkk_day)

        if weekday <= 4:
            # 1) Check time
            time_ok = False
            for session in self.trade_sessions:
                if int(session['open']) <= bkk_time <= int(session['close']) \
                        and ('check_last_bar' in kwargs and kwargs['check_last_bar'] == True\
                             and bkk_time <= int(session['last_bar'])):
                    time_ok = True
                    break

            # 2) Check holiday
            holiday_ok = False
            if str(bkk_day) not in self.set_holidays:
                holiday_ok = True

            result = True if time_ok and holiday_ok else False
        return result


    def is_market_open(self, **kwargs):
        return self.is_trade_time(check_last_bar=False)


    def backfill_price(self, **kwargs):
        """
        backfill ราคาย้อนหลังได้สูงสุดแค่ N แท่ง เท่านั้น ดูฟีลด์ชื่อ backfill_price_count ค่า default คือ 5,000 แท่ง หรือราว 3 เดือนครึ่ง
        ดังนั้นไม่ควรปิดบอทหยุดเทรดเกิน 3 เดือนครึ่ง หากเกินหรือไม่แน่ใจ ต้อง backfill ราคาแบบ historical data ก่อน
        แล้วจึง backfill แบบนี้ต่อ ไม่งั้นข้อมูลอาจแหว่งหายบางช่วงได้
        """
        result = False
        symbol = None
        tf = None

        try:
            db_host = self.robot_config['database_host']
            db_port = self.robot_config['database_port']
            market = self.robot_config['market'].lower()
            broker_id = self.robot_config['broker_id'].lower()

            # <symbol>|<TF> ที่ backfill แล้วจะเก็บใส่ลิสต์นี้ หากเช็กว่ามี <symbol>|<TF> อยู่ในลิสต์นี้แล้วจะไม่ backfill ซ้ำ
            backfill_already_list = []

            for symbol_tf in self.symbol_tf_list:
                items = symbol_tf.split(self.symbol_tf_delimiter)
                symbol = items[0]
                tf = items[1]

                if symbol_tf not in backfill_already_list:
                    collection_name = '{}/{}/{}'.format(self.histdata_collection, symbol, tf)

                    # Get price from Google Cloud Firestore
                    firestore_db = firestore.client()
                    doc_ref = firestore_db.collection(collection_name)\
                        .order_by('datetime', direction='ASCENDING')\
                        .limit(self.backfill_price_count)

                    # append แต่ละ bar (candle) ใส่ลิสต์ โดย 1 bar / 1 document
                    price_dict_arr = []
                    for doc in doc_ref.stream():
                        price_dict_arr.append(doc.to_dict())

                    price_df = pd.DataFrame(price_dict_arr)
                    price_json = price_df.to_json(orient='records')

                    # Backfill
                    result = db_gateway.backfill(db_host, db_port, market, broker_id, symbol, tf, price_json)

                    # เก็บ <symbol>|<TF> ลงลิสต์
                    backfill_already_list.append(symbol_tf)

        except Exception as e:
            raise ExecutionError('Backfill price fail for {}|{} -> {}'.format(symbol, tf, e))

        return result


    def build_sub_orders(self, **kwargs):
        order = kwargs['order']

        pos_size_list = self.__build_random_pos_size_list(order['volume'])

        return pos_size_list


    def get_price(self, account_conf, **kwargs):
        # Query price(s) from database
        return None


    def get_account_info(self, **kwargs):
        if 'exec_params' in kwargs:
            exec_params = kwargs['exec_params']
            account_conf = exec_params['trade_data']['account_conf']
            http_header = exec_params['trade_data']['http_header']
        else:
            account_conf = kwargs['account_conf']
            http_header = kwargs['http_header']

        account_info = None
        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/account-info')
        url = url.format(account_conf['broker_id'], account_conf['account_number'])
        response = self.send_http_get_request(url, header_dict=http_header)
        if response is not None and response.status_code == 200:
            account_info = json_util.decode(response)
        return account_info


    def get_portfolio(self, **kwargs):
        if 'exec_params' in kwargs:
            exec_params = kwargs['exec_params']
            account_conf = exec_params['trade_data']['account_conf']
            http_header = exec_params['trade_data']['http_header']
        else:
            account_conf = kwargs['account_conf']
            http_header = kwargs['http_header']

        portfolio = None
        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/portfolios')
        url = url.format(account_conf['broker_id'], account_conf['account_number'])
        response = self.send_http_get_request(url, header_dict=http_header)
        if response is not None and response.status_code == 200:
            portfolio = json_util.decode(response)
        return portfolio


    def get_positions(self, **kwargs):
        if 'exec_params' in kwargs:
            exec_params = kwargs['exec_params']
            account_conf = exec_params['trade_data']['account_conf']
            http_header = exec_params['trade_data']['http_header']
        else:
            account_conf = kwargs['account_conf']
            http_header = kwargs['http_header']

        symbol = None
        if 'symbol' in kwargs:
            symbol = kwargs['symbol']

        map_to_internal_position = False
        if 'map_to_internal_position' in kwargs:
            map_to_internal_position = kwargs['map_to_internal_position']

        positions = []
        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/portfolios')
        url = url.format(account_conf['broker_id'], account_conf['account_number'])
        response = self.send_http_get_request(url, header_dict=http_header)
        if response is not None and response.status_code == 200:
            pos_list = json_util.decode(response)
            if pos_list is not None and len(pos_list) > 0:
                for pos in pos_list:
                    if symbol == None or (symbol is not None and pos['symbol'] == symbol):
                        if map_to_internal_position == True:
                            # Map ฟีลด์ เพื่อเปลี่ยนชื่อและ type ของฟีลด์เป็นแบบที่ระบบภายในใช้
                            pos = self._map_position(pos)
                        positions.append(pos)
        return positions


    def get_trade_by_id(self, **kwargs):
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ไม่ได้ใช้
        return None


    def get_trades(self, **kwargs):
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ไม่ได้ใช้
        return None

    def close_trade(self, **kwargs):
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ไม่ได้ใช้
        return None


    def modify_trade(self, **kwargs):
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ไม่ได้ใช้
        return None


    def open_position(self, **kwargs):
        exec_params = kwargs['exec_params']
        order_type = 'OPEN'
        result = self.__place_order(order_type, exec_params=exec_params)
        return result


    def close_position(self, **kwargs):
        exec_params = kwargs['exec_params']
        order_type = 'CLOSE'
        result = self.__place_order(order_type, exec_params=exec_params)
        return result


    def modify_position(self, **kwargs):
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ไม่ได้ใช้
        return False


    def get_order_by_id(self, order_id, **kwargs):
        if 'exec_params' in kwargs:
            exec_params = kwargs['exec_params']
            account_conf = exec_params['trade_data']['account_conf']
            http_header = exec_params['trade_data']['http_header']
        else:
            account_conf = kwargs['account_conf']
            http_header = kwargs['http_header']

        order = None
        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/orders/{}')
        url = url.format(account_conf['brokerId'], account_conf['account_number'], order_id)
        response = self.send_http_get_request(url, header_dict=http_header)
        if response is not None and response.status_code == 200:
            order = json_util.decode(response)
        return order


    def get_orders(self, **kwargs):
        if 'exec_params' in kwargs:
            exec_params = kwargs['exec_params']
            account_conf = exec_params['trade_data']['account_conf']
            http_header = exec_params['trade_data']['http_header']
        else:
            account_conf = kwargs['account_conf']
            http_header = kwargs['http_header']

        orders = []
        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/orders')
        url = url.format(account_conf['broker_id'], account_conf['account_number'])
        response = self.send_http_get_request(url, header_dict=http_header)
        if response is not None and response.status_code == 200:
            orders = json_util.decode(response)
        return orders


    def cancel_order_by_id(self, order_id, **kwargs):
        result = False

        if 'exec_params' in kwargs:
            exec_params = kwargs['exec_params']
            account_conf = exec_params['trade_data']['account_conf']
            http_header = exec_params['trade_data']['http_header']
        else:
            account_conf = kwargs['account_conf']
            http_header = kwargs['http_header']

        retry = 0
        cancel_success = False
        while cancel_success != True and retry < self.max_retry:
            payload_dict = { 'pin': account_conf['pin'] }
            payload_json = json_util.encode(payload_dict)

            url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/orders/{}/cancel')
            url = url.format(account_conf['broker_id'], account_conf['account_number'], order_id)
            cancel_response = self.send_http_patch_request(url, payload_json, header_dict=http_header)

            # Get latest order by order no
            order = self.get_order_by_id(order_id, exec_params=exec_params)
            if cancel_response is not None and cancel_response.status_code == 200\
                    and order is not None and self.is_active_order(order['status']) == False:
                cancel_success = True
                result = True
            else:
                time.sleep(self.wait_seconds)
                retry = retry + 1

        if retry == self.max_retry and cancel_success == False:
            raise Exception('Cancel order(s) fail')
        return result


    def cancel_orders(self, **kwargs):
        result = False

        if 'exec_params' in kwargs:
            exec_params = kwargs['exec_params']
            account_conf = exec_params['trade_data']['account_conf']
            http_header = exec_params['trade_data']['http_header']
            symbol = exec_params['trade_data']['symbol']
            orders = self.__get_orders_by_symbol(exec_params['trade_data']['orders'], symbol)
        else:
            account_conf = kwargs['account_conf']
            http_header = kwargs['http_header']
            symbol = kwargs['symbol']
            orders = kwargs['orders']


        if orders is not None and len(orders) > 0:
            # Add order no ที่จะ cancel ใส่ list
            order_nos = []
            for order in orders:
                if self.__is_active_order(order['status']) == True:
                    if symbol == None or (symbol is not None and order['symbol'] == symbol):
                        # Add order no. to be canceled to list
                        order_nos.append(order['orderNo'])

            order_nos_len = len(order_nos)
            if order_nos_len > 0:
                retry = 0
                cancel_success = False
                while cancel_success != True and retry < self.max_retry:
                    if order_nos_len > 1:
                        payload_dict = { 'orders': order_nos, 'pin': account_conf['pin'] }
                        payload_json = json_util.encode(payload_dict)

                        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/orders/{}/cancel')
                        url = url.format(account_conf['broker_id'], account_conf['account_number'])
                        cancel_response = self.send_http_patch_request(url, payload_json, header_dict=http_header)

                        # Wait
                        time.sleep(self.wait_order_exec_seconds)

                        # Get latest order list
                        orders = self.get_orders(exec_params=exec_params)
                        if cancel_response is not None and cancel_response.status_code == 207 \
                                and orders is not None and self.__has_active_orders_by_symbol(orders, symbol) == False:
                            cancel_success = True
                            result = True
                        else:
                            time.sleep(self.wait_seconds)
                            retry = retry + 1

                if retry == self.max_retry and cancel_success == False:
                    raise Exception('Cancel order(s) fail')
        else:
            # ไม่มี position หรือ มี position แต่ไม่มีออร์เดอร์สถานะ pending หรือ queuing เลย จึงไม่ต้อง cancel order
            result = True
        return result


    def modify_order(self, **kwargs):
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ไม่ได้ใช้
        return None


    def send_stop_order(self, **kwargs):
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ไม่ได้ใช้
        return None


    def cancel_stop_order(self, **kwargs):
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ไม่ได้ใช้
        return None
    # ==================================================================================================
    # END: Fine-grained execution helper methods
    # ==================================================================================================


# ======================================================================================================
def main(argv):
    robot_config = None
    # 1) Load robot configuration file
    try:
        robot_config_file = ''
        opts, args = getopt.getopt(argv, "hf:", ["file="])

        for opt, arg in opts:
            if opt == '-h':
                print('tfex_exec.py -f <path_of_robot_config.yaml> or tfex_exec.py --file=<path_of_robot_config.yaml>'
                      + ' or tfex_exec.py --file <path_of_robot_config.yaml>')
                sys.exit()
            elif opt in ("-f", "--file"):
                robot_config_file = arg
        robot_config = yaml.safe_load(open(robot_config_file))
    except:
        print('Start failed')
        print('Usage:')
        print('tfex_exec.py -f <path_of_robot_config.yaml> or tfex_exec.py --file=<path_of_robot_config.yaml>'
              + ' or tfex_exec.py --file <path_of_robot_config.yaml>')
        sys.exit(2)

    # 2) Start execution
    try:
        exec = TfexExec(robot_config)
        exec.start()
    except ExecutionError as e:
        print(e)
# ======================================================================================================


if __name__ == "__main__":
   main(sys.argv[1:])
