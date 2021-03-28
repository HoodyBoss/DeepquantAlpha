from importlib import import_module

import multiprocessing
import time

import deepquant.common.datetime_util as datetime_util
import deepquant.common.cache_proxy as cache_proxy
import deepquant.common.json_util as json_util
import deepquant.common.http_util as http_util
import deepquant.common.line_notify as line_notify
from deepquant.common.state_machine import StateMachine

import deepquant.data.db_gateway as db_gateway

from deepquant.ems.error import ExecutionError


class BaseExec():

    def __init__(self, **kwargs):
        self.robot_config = kwargs['robot_config']

        self.symbol_tf_delimiter = '|'  # ตัวอักษรคั่นระหว่าง symbol กับ timeframe มีค่า default คือตัวอักษร '|'

        self.quantity_digits = self.robot_config['quantity_digits'] # จำนวนหน่วยทศนิยมของ position size เช่น สัญญา มีค่า default คือ 0
        self.backfill_price_count = self.robot_config['backfill_price_count']  # จ.น.แท่งเทียนที่โหลดตอน backfill มีค่า default คือ 5,000 แท่ง
        self.live_price_count = self.robot_config['live_price_count']  # จ.น.แท่งเทียนที่โหลดตอนรันเทรดจริง มีค่า default คือ 12 แท่ง

        self.base_bar_interval = self.robot_config['base_bar_interval']  # default คือ 5 นาที
        self.run_interval_sleep = self.robot_config['run_interval_sleep']  # default คือ 5 วินาที
        self.max_missed_execute = self.robot_config['max_missed_execute']  # default คือ 5 นาที

        self.max_retry = self.robot_config['max_retry']
        self.wait_seconds = self.robot_config['wait_seconds']
        self.wait_order_exec_seconds = self.robot_config['wait_order_exec_seconds']
        # พิจารณาจากสภาพคล่อง เช่น ไม่เกิน 25 สัญญาต่อออร์เดอร์
        self.pos_size_liquid_limit = self.robot_config['pos_size_liquid_limit']
        # เช่น 0.25 -> 25%
        self.init_reduce_pos_size_percent = self.robot_config['init_reduce_pos_size_percent']

        self.base_token_env_var = self.robot_config['base_token_env_var']
        self.base_domain = self.robot_config['base_domain']
        self.http_header = {}

        # Trade sessions (list of dictionary)
        # Sample YAML config:
        # trade_sessions:
        # - open : 94500
        #   close : 123000
        #   last_bar : 122500
        # - open : 141500
        #   close : 165500
        #   last_bar : 165000})
        #
        # Sample config for 24 hours
        # trade_sessions:
        # - open : 000000
        #   close : 240000
        #   last_bar: ต้องระบุเอง เพราะขึ้นกับ time frame ที่ใช้ เช่นใช้ M5 ก็ระบุค่า 235500
        self.trade_sessions = self.robot_config['trade_sessions']


        # key คือ <symbol>|<TF> เช่น 'S50|M5', value คือ dict {datetime, open, high, low, close, volume}
        self.latest_prices = {}
        # key คือ <symbol>|<TF> เช่น 'S50|M5', value คือ dict {datetime, open, high, low, close, volume}
        self.prev_exec_prices = {}  # เก็บราคาของทุก <symbol>|<TF> ที่ประมวลผล bar ล่าสุด
        # key คือ <symbol>|<TF> เช่น 'S50|M5', value คือ dict {datetime, open, high, low, close, volume}
        self.prev_exec_sltp_prices = {}  # เก็บราคาของทุก <symbol>|<TF> ที่ประมวลผล SL/TP ล่าสุด

        self.cur_correl_id = None
        self.prev_correl_id = None
        self.base_time = None           # initialize ในเมธอด build_trade_input()
        self.symbol_tf_list = None      # initialize ในเมธอด init_robot_positions() เป็น list of string <symbol>_<TF>
        self.robot_labels = None        # initialize ในเมธอด init_robot_positions() เป็น dictionary, key คือ robot name
        self.robot_symbol_infos = None  # initialize ในเมธอด init_robot_positions() เป็น dictionary, key คือ robot name

        self.ml_models = {}
        print('Initialize based execution finished')


    # ==================================================================================================
    # BEGIN: Core method
    # ==================================================================================================
    def execute(self, correl_id, **kwargs):
        # NOTE: 1 account / 1 trading robot, 1 trading robot / 1 symbol

        # Datetime format สำหรับ server_time, local_time และ base_time คือ %Y%m%d%H%M%S เช่น 20200824171500 เป็น string
        server_time = datetime_util.utcnow().strftime('%Y%m%d%H%M%S')
        local_time = datetime_util.local_now().strftime('%Y%m%d%H%M%S')

        # 1) Build trading input and run data pipeline for preparing datasets
        trade_input = self.build_trade_input(correl_id=correl_id, server_time=server_time, local_time=local_time)

        # 2) Handle trading predictions
        trade_actions = self.handle_predictions(trade_input)

        # 3) Handle trading actions
        self.handle_trade_actions(trade_input, trade_actions)


    def execute_sltp(self, **kwargs):
        try:
            # Datetime format สำหรับ server_time, local_time และ base_time คือ %Y%m%d%H%M%S เช่น 20200824171500 เป็น string
            server_time = datetime_util.utcnow().strftime('%Y%m%d%H%M%S')
            local_time = datetime_util.local_now().strftime('%Y%m%d%H%M%S')
            self.base_time = local_time  # Use local time

            # 1) Generate correlation ID
            correl_id = self.gen_correl_id()

            # 2) Load trading state (position)
            # Initialize cache proxy
            cache = cache_proxy.CacheProxy(self.robot_config['cache_host'], self.robot_config['cache_port'])
            self.load_trading_state(cache=cache)

            # 3) Parallelize the SL/TP execution, 1 process / 1 trading robot
            processes = []
            for tr_robot_conf in self.robot_config['trading_robots']:
                robot_name = tr_robot_conf['name']
                label = self.robot_labels[robot_name]
                symbol = self.robot_symbol_infos[robot_name]['name']
                timeframe = tr_robot_conf['timeframe']

                # NOTE !!!! MUST BE Edit later
                account_number = self.robot_config['account_number']

                process = multiprocessing.Process(target=self._execute_sltp_task, args=(account_number
                                                                                        , correl_id
                                                                                        , server_time
                                                                                        , local_time
                                                                                        , self.base_time
                                                                                        , label
                                                                                        , symbol
                                                                                        , timeframe,))
                processes.append(process)

            for p in processes:
                p.start()
        except Exception as e:
            raise ExecutionError('Execute SL/TP fail -> {}'.format(e))
    # ==================================================================================================
    # END: Core method
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Task method
    # ==================================================================================================
    def _execute_sltp_task(self, account_number, correl_id, server_time, local_time, base_time
                           , label, symbol, timeframe):
        """
        เมธอดนี้เน้นประมวลผลเร็ว โดยจะรับราคาล่าสุดมาตรวจสอบ หากเข้าเงื่อนไขจะส่งส่ง close trade ทันที
        ดังนั้นจึงไม่ได้ใช้ trade_input แบบในเมธอดอื่นๆ เพราะจะเสียเวลาโหลดข้อมูลต่างๆ เพื่อสร้าง trade_input ซึ่งส่วนใหญ่ไม่ได้ใช้ในเมธอดนี้
        """
        found_close = False

        # Load latest price only 1 row (1 bar)
        price_df = self.get_price(symbol=symbol, timeframe=timeframe, count=1)
        price = price_df.iloc[len(price_df) - 1]

        # Initialize cache proxy
        cache = cache_proxy.CacheProxy(self.robot_config['cache_host'], self.robot_config['cache_port'])
        
        # Get position from cache
        position = cache.get('pos_{}'.format(label))

        if position is not None\
            and (position['trade_type'] == 'BUY' or position['trade_type'] == 'SELL')\
            and position['quantity'] > 0 and position['stop_loss'] > 0 and position['take_profit'] > 0:

            if position['trade_type'] == 'BUY'\
                and (price['low'] <= position['stop_loss'] or price['close'] <= position['stop_loss']\
                    or price['high'] >= position['take_profit'] or price['close'] >= position['take_profit']):
                # Close
                self.close_position(account_number=account_number, label=label, quantity=position['quantity'])
                found_close = True

            elif position['trade_type'] == 'SELL'\
                and (price['high'] >= position['stop_loss'] or price['close'] >= position['stop_loss']\
                    or price['low'] <= position['take_profit'] or price['close'] <= position['take_profit']):
                # Close
                self.close_position(account_number=account_number, label=label, quantity=position['quantity'])
                found_close = True
        return found_close


    def _handle_action_task_common(self, trade_action, position, exec_params):
        sm = StateMachine

        cache = exec_params['cache']

        action_code = trade_action['action_code']
        symbol_tick_size = exec_params['symbol_info']['tick_size']

        notify_msg = ''

        if action_code in [sm.ACTION_OPEN_BUY, sm.ACTION_OPEN_SELL]:
            try:
                # 1) Close position
                if position is not None and position['quantity'] > 0:
                    self.close_position(exec_params=exec_params)
                    # Reset robot's SL & TP
                    self.reset_cache_robot_position(cache, trade_action['label'], trade_action['robot_name']
                                                    , trade_action['symbol_name'])

                # 2) Wait a seconds
                time.sleep(self.wait_order_exec_seconds)
                # 3) Open position
                self.open_position(exec_params=exec_params)
                # Memorize stop loss and take profit
                self.memorize_cache_robot_position(cache, trade_action['label'], trade_action['robot_name']
                                                   , trade_action['symbol_name'], trade_action['trade_type']
                                                   , trade_action['quantity']
                                                   , trade_action['stop_loss']
                                                   , trade_action['take_profit'])
                # 4) Wait a seconds
                time.sleep(self.wait_order_exec_seconds)
                # 5) Get latest positions and then get active position for this symbol
                latest_position = self.__get_latest_position(self, exec_params, symbol_tick_size)
                if latest_position is not None and latest_position['quantity'] > 0:
                    # Write trading position to database
                    self.save_trading_state(exec_params=exec_params)
                else:
                    raise ExecutionError('Open new position successful but trade is invalid')

                notify_msg = ''
            except ExecutionError as e:
                raise ExecutionError('Open new position fail -> {}'.format(e))

        # ==========================================================================================
        elif action_code == sm.ACTION_MODIFY_POSITION:
            # Handle trailing stop or modify take profit
            try:
                # Memorize stop loss and take profit
                self.memorize_cache_robot_position(cache, trade_action['label'], trade_action['robot_name']
                                                   , trade_action['symbol_name'], trade_action['trade_type']
                                                   , trade_action['quantity']
                                                   , trade_action['stop_loss']
                                                   , trade_action['take_profit'])

                notify_msg = ''
            except ExecutionError as e:
                raise ExecutionError('Open new position fail -> {}'.format(e))

        # ==========================================================================================
        elif action_code in [sm.ACTION_CLOSE_BUY, sm.ACTION_CLOSE_SELL]:
            try:
                # 1) Close position
                self.close_position(exec_params=exec_params)
                # 2) Wait a seconds
                time.sleep(self.wait_order_exec_seconds)
                # 3) Get latest trade and then validate trading status
                latest_position = self.__get_latest_position(self, exec_params, symbol_tick_size)
                if latest_position is not None and latest_position['quantity'] > 0:
                    # Reset robot's SL & TP
                    self.reset_cache_robot_position(cache, trade_action['label'], trade_action['robot_name']
                                                    , trade_action['symbol_name'])
                else:
                    raise ExecutionError('Close position successful but position is still valid')

                notify_msg = ''
            except ExecutionError as e:
                raise ExecutionError('Close position fail -> {}'.format(e))

        # ==========================================================================================
        elif action_code in [sm.ACTION_SCALE_OUT_BUY, sm.ACTION_SCALE_OUT_SELL]:
            try:
                # 1) Close position
                self.close_position(exec_params=exec_params)
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
                        raise ExecutionError("Scale out position successful but the quantity has not been changed -> label:{}".format(
                            trade_action['label']))

                notify_msg = ''
            except ExecutionError as e:
                raise ExecutionError('Scale out fail -> {}'.format(e))

        return notify_msg
    # ==================================================================================================
    # END: Task method
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Coarse-grained execution helper methods
    # ==================================================================================================
    def build_trade_input(self, **kwargs):
        """
        Build trading input
        :return: returns dictionary
        """
        correl_id = kwargs['correl_id']
        server_time = kwargs['server_time']
        local_time = kwargs['local_time']

        # 1) Load price
        # Format of symbol_prices is dictionary of JSON. Dictionary key is <SYMBOL|TF>
        symbol_prices = self._get_symbol_prices(self.symbol_tf_list)
        # Uses datetime of first trading robot as base_time. Format is %Y%m%d%H%M%S เช่น 20200824171500
        self.base_time = str(symbol_prices[list(symbol_prices.keys())[0]]['datetime'])

        # 2) Load full account details from broker
        original_account = self.get_account_info()
        account_positions = original_account['trades']
        # ไม่ใช้ list of order
        # orders = oanda_account['orders']

        # 4) Load trading robot configurations
        trading_robot_config_list = self.robot_config['trading_robots']

        # 5) Create account info - โดยใช้คำแบบในระบบเรา เพื่อใช้ส่งเข้าไปในระบบภายใน
        # ส่วนฟีลด์ account ทั้งหมดที่โหลดมาจาก OANDA เก็บในตัวแปร oanda_account ซึ่งจะส่งเข้าไปในระบบภายในด้วย เพื่อให้เรียกใช้ได้อิสระ
        account_info = self._map_account(self, self.robot_config['account_number'], original_account)

        trade_input = {
            'correl_id': correl_id
            , 'strategy_name': self.robot_config['strategy_name']
            , 'base_time': self.base_time
            , 'server_time': server_time
            , 'local_time': local_time
            , 'account': account_info
            , 'original_account': original_account
            , 'prices': symbol_prices
        }

        # 6) Set specific data for each trading robots under this account
        labels = {}  # key is trading robot name
        positions = {}  # key is trading robot name
        symbol_infos = {}  # key is trading robot name
        for tr_robot_config in trading_robot_config_list:
            tr_robot_name = tr_robot_config['name']
            label = self.robot_labels[tr_robot_name]
            symbol_info = self.robot_symbol_infos[tr_robot_name]
            positions[tr_robot_name] = self._map_position(self._get_active_position(account_positions
                                                                                    , label=label
                                                                                    , symbol=symbol_info['name']))

            symbol_infos[tr_robot_name] = symbol_info
            labels[tr_robot_name] = label

        trade_input['positions'] = positions
        trade_input['symbol_infos'] = symbol_infos
        trade_input['labels'] = labels

        # 7) Call data pipeline to start flow of dataset preparation
        # The 'ml_models' performs like a ML models cache
        class_pipeline = getattr(import_module(self.robot_config['data_pipeline_module_path']
                                               , self.robot_config['data_pipeline_class']))
        pipeline = class_pipeline(self.robot_config, symbol_prices, self.ml_models, correl_id=correl_id)
        pipeline.start_flow()
        trade_input['datasets'] = pipeline.datasets

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
                                  , trade_input['base_time']
                                  , trade_input['server_time']
                                  , trade_input['local_time']
                                  , trade_input['account']
                                  , trade_input['positions']
                                  , self.ml_models
                                  , original_account=trade_input['original_account'])

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
        """
        Handle trading actions(s)
        :return: returns correlation ID
        """
        return None
    # ==================================================================================================
    # END: Coarse-grained execution helper methods
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Trading state caching and persistence methods
    # ==================================================================================================
    def init_robot_positions(self, **kwargs):
        """
        Initialize trading position of all trading robots
        robot position schema -> { label, robot_name, trade_type, quantity, stop_loss, take_profit }
        """
        # Initialize cache proxy
        cache = cache_proxy.CacheProxy(self.robot_config['cache_host'], self.robot_config['cache_port'])

        symbol_id_index_dict = self.get_symbol_id_index()
        self.symbol_tf_list = self.get_trade_symbol_tf_list(symbol_id_index_dict)

        self.robot_labels = {}
        self.robot_symbol_infos = {}
        tr_robot_configs = self.robot_config['trading_robots']
        for conf in tr_robot_configs:
            # Create label (position ID) and save to execution instance
            label = '{}{}{}{}'.format(self.robot_config['strategy_id'], conf['id'], conf['symbol'], conf['trade_model_id'])

            robot_name = conf['name']
            self.robot_labels[robot_name] = label
            symbol_index = int(symbol_id_index_dict[str(conf['symbol'])])
            self.robot_symbol_infos[robot_name] = self.robot_config['symbols'][symbol_index]

            position = { 'label' : label
                        , 'robot_name' : conf['name']
                        , 'symbol_name' : self.robot_symbol_infos[robot_name]['name']
                        , 'trade_type' : 'NONE'
                        , 'quantity' : 0.0
                        , 'stop_loss' : 0.0
                        , 'take_profit' : 0.0 }

            # Store each position to cache
            position_json = json_util.encode(position)
            cache.set('pos_{}'.format(label), position_json)

        # This is a tricky step.
        # Load trading state (positions) from database into cache
        # ภายในเมธอด load_trading_state() เมื่อโหลด trading position ของทุก trading robot มาแล้ว
        # จะ store ลง cache ทับอีกที
        # เพื่อรองรับกรณีเช่น ผู้ใช้ปิดบอทชั่วคราวด้วยสาเหตุใดก็ตาม เช่น รีสตาร์ทเครื่อง แล้วเริ่มรันบอทใหม่
        # จะได้โหลด trading position ที่เก็บไว้ใน database ขึ้นมาแล้วเซ็ตค่าทับลงใน cache จะได้มีข้อมูลที่ตรงกัน
        self.load_trading_state(cache=cache)


    def memorize_cache_robot_position(self, cache, label, robot_name, symbol, trade_type
                                        , quantity, stop_loss, take_profit, **kwargs):
        # Save stop loss, take profit, trade type และ quantity ของ trading robot ทุกตัวเก็บลง cache
        position = { 'label': label
                    , 'robot_name': robot_name
                    , 'symbol_name' : symbol
                    , 'trade_type': trade_type
                    , 'quantity': quantity
                    , 'stop_loss': stop_loss
                    , 'take_profit': take_profit }
        # Store each position to cache
        position_json = json_util.encode(position)
        cache.set('pos_{}'.format(label), position_json)


    def reset_cache_robot_position(self, cache, label, robot_name, symbol, **kwargs):
        # Reset stop loss, take profit และ quantity ของ trading robot ทุกตัวเป็นค่าศูนย์ และ reset trade type เป็น NONE
        position = { 'label': label
                    , 'robot_name': robot_name
                    , 'symbol_name' : symbol
                    , 'trade_type': 'NONE'
                    , 'quantity': 0.0
                    , 'stop_loss': 0.0
                    , 'take_profit': 0.0 }
        # Store each position to cache
        position_json = json_util.encode(position)
        cache.set('pos_{}'.format(label), position_json)

    def get_cache_robot_position(self, cache, label, **kwargs):
        position_from_cache = cache.get('pos_{}'.format(label))
        if type(position_from_cache) == bytes:
            position_str = position_from_cache.decode('utf-8')
            position = json_util.decode(position_str)
        else:
            position = json_util.decode(position_from_cache)
        return position
    # ==================================================================================================
    # END: Trading state caching and persistence methods
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Utility methods
    # ==================================================================================================
    def get_trade_symbol_tf_list(self, symbol_id_index_dict):
        symbol_tf_list = []
        for tr_bot_conf in self.robot_config['trading_robots']:
            # Get symbol config
            symbol_index = int( symbol_id_index_dict[str(tr_bot_conf['symbol'])] )

            symbol_name = self.robot_config['symbols'][symbol_index]['name']
            tf = tr_bot_conf['timeframe']
            symbol_tf = '{}{}{}'.format(symbol_name, self.symbol_tf_delimiter, tf)
            symbol_tf_list.append(symbol_tf)
        return symbol_tf_list


    def get_symbol_id_index(self):
        symbol_id_index_dict = {}
        for i in range(0, len(self.robot_config['symbols'])):
            symbol_id = self.robot_config['symbols'][i]['id']
            symbol_id_index_dict[str(symbol_id)] = i
        return symbol_id_index_dict


    def send_http_post_request(self, url, json_request_payload, header_dict=None):
        req_data = json_util.encode(json_request_payload)
        if header_dict is not None:
            response = http_util.post(url, req_data, header_dict=header_dict, require_response=True)
        else:
            response = http_util.post(url, req_data, header_dict=self.http_header, require_response=True)
        return response


    def send_http_patch_request(self, url, json_request_payload, header_dict=None):
        req_data = json_util.encode(json_request_payload)
        if header_dict is not None:
            response = http_util.patch(url, req_data, header_dict=header_dict, require_response=True)
        else:
            response = http_util.patch(url, req_data, header_dict=self.http_header, require_response=True)
        return response


    def send_http_get_request(self, url, header_dict=None):
        if header_dict is not None:
            response = http_util.get(url, header_dict=header_dict)
        else:
            response = http_util.get(url, header_dict=self.http_header)
        return response


    def send_http_put_request(self, url, json_request_payload, header_dict=None):
        req_data = json_util.encode(json_request_payload)
        if header_dict is not None:
            response = http_util.put(url, req_data, header_dict=header_dict, require_response=True)
        else:
            response = http_util.put(url, req_data, header_dict=self.http_header, require_response=True)
        return response
    # ==================================================================================================
    # END: Utility methods
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Protected methods
    # ==================================================================================================
    def _get_symbol_prices(self, symbol_tf_list):
        """
        Returns dictionary of JSON, dictionary key is <SYMBOL|TF>
        """
        return None


    def _get_active_position(self, positions, label=None, symbol=None, **kwargs):
        """
        Returns active position (open trade) for the specified label or symbol
        """
        trade = None
        return trade


    def _map_account(self, account_number, original_account, **kwargs):
        """
        Map original account fields that get from broker to internal system account fields
        """
        account_info = {}
        return account_info


    def _map_position(self, original_position, symbol_tick_size, **kwargs):
        """
        Map original account fields that get from broker to internal system account fields
        """
        position = {}
        return position
    # ==================================================================================================
    # END: Protected methods
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Fine-grained execution helper methods
    # ==================================================================================================
    def is_trade_time(self, **kwargs):
        """
        Check trade time including check trade day, weekend, holiday and trade time configuration
        :return: True is trade time, otherwise False
        """
        return True


    def is_market_open(self, **kwargs):
        """
        Returns True if market is open, otherwise False
        """
        return True


    def require_backfill(self):
        result = False
        dt_format = '%Y%m%d%H%M%S'
        if self.prev_correl_id == None:
            result = True
        else:
            # Remove 2 chars representing strategy ID
            diff = datetime_util.minutes_diff(self.prev_correl_id[2:len(self.prev_correl_id)]
                                              , self.cur_correl_id[2:len(self.cur_correl_id)]
                                              , dt_format)
            if diff > self.max_missed_execute:
                result = True
        return result


    def backfill_price(self, **kwargs):
        """
        Backfill price data
        :return: True if successful, otherwise False
        """
        return None


    def gen_correl_id(self, **kwargs):
        """
        Generate new correlation ID
        :return: returns string
        """
        correl_id = '{}{}00'.format(self.robot_config['strategy_id']
                                  , datetime_util.bangkok_now().strftime('%Y%m%d%H%M'))
        return correl_id


    def stamp_finish(self, **kwargs):
        """
        Update timestamp of latest interval after execution finished
        :return: True if successful, otherwise False
        """
        # อัพเดต timestamp ของการ execute รอบล่าสุด
        dt = self.base_time
        base_time = '{}-{}-{} {}:{}:{}'.format(dt[0:4], dt[4:6], dt[6:8], dt[8:10], dt[10:12], dt[12:14])
        self.last_executed_bar_time = base_time

        measurement_name = 'last_bar_time'

        # ค่า time ของ measurement นี้มีค่า '2020-01-01T00:00:00Z' เสมอ เพราะต้องการให้ measurement นี้มีแค่ point (row) เดียว
        data = [{
            "measurement": measurement_name,
            "tags": {
                "strategy_name": self.robot_config['strategy_name'].lower()
            },
            "fields": {
                "bar_time" : base_time
            },
            "time": '2020-01-01T00:00:00Z'
        }]

        result = db_gateway.write_time_series_daa(self.robot_config['database_host']
                                                    , self.robot_config['database_port']
                                                    , self.robot_config['market']
                                                    , data, time_precision='ms')
        return result


    def save_trading_state(self, **kwargs):
        """
        Save latest state of trading position(s)
        :return: True if successful, otherwise False
        """
        if 'exec_params' in kwargs:
            exec_params = kwargs['exec_params']
        else:
            raise ExecutionError('Save trading state fail -> invalid exec_params')

        if 'cache' in exec_params:
            cache = exec_params['cache']
        else:
            raise ExecutionError('Save trading state fail -> invalid cache')

        if 'trade_input' in exec_params :
            trade_input = kwargs['trade_input']
        else:
            raise ExecutionError('Save trading state fail -> invalid trade_input')

        labels = trade_input['labels']

        measurement_name = 'position'

        for robot_name in list(labels.keys()):
            label = labels[robot_name]
            # 1) Get robot position from cache
            position = self.get_cache_robot_position(cache, label)
            if position is not None:
                # 2) Set time
                now = datetime_util.utcnow()
                timestamp = now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

                # 3) Insert/update position
                data = [{
                    "measurement": measurement_name,
                    "tags": {
                        'label': label
                        , 'robot_name': robot_name
                        , 'symbol_name' : position['symbol_name']
                    },
                    "fields": {
                        'trade_type': position['trade_type']
                        , 'quantity': position['quantity']
                        , 'stop_loss': position['stop_loss']
                        , 'take_profit': position['take_profit']
                    },
                    "time": timestamp
                }]

                db_gateway.write_time_series_data(self.robot_config['database_host']
                                                    , self.robot_config['database_port']
                                                    , self.robot_config['market']
                                                    , data, time_precision='ms')
        return True


    def reset_trading_state(self, **kwargs):
        """
        Reset state of trading position(s)
        :return True if successful, otherwise False
        """
        if 'exec_params' in kwargs:
            exec_params = kwargs['exec_params']
        else:
            raise ExecutionError('Reset trading state fail -> invalid exec_params')

        if 'trade_input' in exec_params:
            trade_input = kwargs['trade_input']
        else:
            raise ExecutionError('Reset trading state fail -> invalid trade_input')

        # Get latest open positions
        positions = self.get_positions(**kwargs)
        labels = trade_input['labels']

        measurement_name = 'position'

        for robot_name in list(labels.keys()):
            label = labels[robot_name]
            symbol = self.robot_symbol_infos[robot_name]['name']
            position = self._get_active_position(positions, label=label, symbol=symbol)

            symbol_info = self.robot_symbol_infos[robot_name]

            # Set time
            now = datetime_util.utcnow()
            timestamp = now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

            if position == None:
                # Set data
                data = [{
                    "measurement": measurement_name,
                    "tags": {
                        'label': label
                        , 'robot_name': robot_name
                        , 'symbol_name': position['symbol_name']
                    },
                    "fields": {
                        'trade_type': 'NONE'
                        , 'quantity': 0.0
                        , 'stop_loss': 0.0
                        , 'take_profit': 0.0
                    },
                    "time": timestamp
                }]
            else:
                # Set data
                data = [{
                    "measurement": measurement_name,
                    "tags": {
                        'label': label
                        , 'robot_name': robot_name
                        , 'symbol_name': position['symbol_name']
                    },
                    "fields": {
                        'trade_type': position['trade_type']
                        , 'quantity': round(position['quantity'], self.quantity_digits)
                        , 'stop_loss': round(position['stop_loss'], symbol_info['digits'])
                        , 'take_profit': round(position['take_profit'], symbol_info['digits'])
                    },
                    "time": timestamp
                }]

            # Insert/update position
            db_gateway.write_time_series_data(self.robot_config['database_host']
                                                , self.robot_config['database_port']
                                                , self.robot_config['market']
                                                , data, time_precision='ms')
        return True


    def load_trading_state(self, **kwargs):
        """
        Returns state of trading position(s)
        :return: list of dictionary of last trading state of all trading robots grouping by account no
        """

        if 'cache' in kwargs:
            cache = kwargs['cache']
        else:
            raise ExecutionError('Save trading state fail -> invalid cache')

        positions = []
        labels = self.robot_labels

        measurement_name = 'position'
        # Query only 1 row for each label
        stmt = 'SELECT * FROM "{}" WHERE label=$label ORDER BY DESC LIMIT 1'.format(measurement_name)

        for robot_name in list(labels.keys()):
            label = labels[robot_name]
            symbol_info = self.robot_symbol_infos[robot_name]

            bind_params = {'label': label}
            results = db_gateway.query(self.robot_config['database_host']
                                            , self.robot_config['database_port']
                                            , self.robot_config['market']
                                            , stmt, bind_params=bind_params)
            if results is not None:
                for point in results: # 'point' เทียบเท่าคำว่า 'record' ที่ใช้ใน database ทั่วไป
                    position = {'label': point['label']
                        , 'robot_name': point['robot_name']
                        , 'symbol_name' : point['symbol_name']
                        , 'trade_type': point['trade_type']
                        , 'quantity': round(point['quantity'], self.quantity_digits)
                        , 'stop_loss': round(point['stop_loss'], symbol_info['digits'])
                        , 'take_profit': round(point['take_profit'], symbol_info['digits'])}
                    positions.append(position)

                    # Update to cache
                    self.memorize_cache_robot_position(cache, point['label'], point['robot_name']
                                                       , point['symbol_name'], point['trade_type']
                                                       , round(point['quantity'], self.quantity_digits)
                                                       , round(point['stop_loss'], symbol_info['digits'])
                                                       , round(point['take_profit'], symbol_info['digits']))
        return positions


    def build_sub_orders(self, **kwargs):
        """
        แบ่งออร์เดอร์ขนาดใหญ่ออกเป็นออร์เดอร์ย่อยๆ โดยแต่ละออร์เดอร์ย่อยมีรายละเอียดเหมือนกับออร์เดอร์ใหญ่เดิมหมด
        ยกเว้น position size
        """
        return None

    def get_price(self, **kwargs):
        """
        Returns price
        :return: returns DataFrame: date, open, high, low, close, volume
        """
        return None

    def get_account_info(self, **kwargs):
        """
        Returns account information
        :return: returns dictionary
        """
        return None

    def get_portfolio(self, **kwargs):
        """
        Returns portfolio information
        :return: returns dictionary
        """
        return None

    def get_positions(self, **kwargs):
        """
        Returns active trading position
        :return: returns list of dictionary
        """
        return None

    def get_trade_by_id(self, **kwargs):
        """
        Returns trade by id
        :return: returns dictionary
        """
        return None

    def get_trades(self, **kwargs):
        """
        Returns trades
        :return: returns list of dictionary
        """
        return None

    def close_trade(self, **kwargs):
        """
        Close active trade
        :return: True if successful, otherwise False
        """
        return None

    def modify_trade(self, **kwargs):
        """
        Modify existing trade
        :return: True if successful, otherwise False
        """

    def open_position(self, **kwargs):
        """
        Open new trading position
        :return: True if successful, otherwise False
        """
        return False

    def close_position(self, **kwargs):
        """
        Close active trading position
        :return: True if successful, otherwise False
        """
        return False

    def modify_position(self, **kwargs):
        """
        Modify active trading position
        :return: True if successful, otherwise False
        """
        return False

    def get_order_by_id(self, order_id, **kwargs):
        """
        Returns order information by order id
        :return: returns dictionary
        """
        return None

    def get_orders(self, **kwargs):
        """
        Returns list of order information
        :return: returns list of dictionary
        """
        return False

    def cancel_order_by_id(self, order_id, **kwargs):
        """
        Cancel active order by order id
        :return: True if successful, otherwise False
        """
        return False

    def cancel_orders(self, **kwargs):
        """
        Cancel active orders
        :return: True if successful, otherwise False
        """
        return False

    def modify_order(self, **kwargs):
        """
        Modify active trading order. Active order status examples: pending, queueing
        :return: True if successful, otherwise False
        """
        return False

    def send_stop_order(self, **kwargs):
        """
        Send stop order. Because some markets needs to send stop order separately
        :return: True if successful, otherwise False
        """
        return False

    def cancel_stop_order(self, **kwargs):
        """
        Cancel active stop order
        :return: True if successful, otherwise False
        """
        return False
    # ==================================================================================================
    # BEGIN: Fine-grained execution helper methods
    # ==================================================================================================
