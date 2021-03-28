import multiprocessing
import time
import math
import random
from importlib import import_module

import deepquant.common.datetime_util as datetime_util
import deepquant.common.line_notify as line_notify
import deepquant.common.json_util as json_util
import deepquant.common.cache_proxy as cache_proxy
from deepquant.common.state_machine import State, StateMachine

import deepquant.data.influxdb_util as influxdb_util

from deepquant.ems.base_exec import BaseExec
from deepquant.ems.error import ExecutionError


class TfexExec(BaseExec):

    def __init__(self, robot_config):
        super().__init__(robot_config)

        self.quantity_digits = 0

        self.base_domain = 'http://localhost:8089/api/seosd/v1'

        self.base_bar_interval = 5 # 5 minutes
        self.run_interval_sleep = 5 # 5 seconds

        self.max_retry = self.robot_config['max_retry']
        self.wait_seconds = self.robot_config['wait_seconds']
        self.wait_order_exec_seconds = self.robot_config['wait_order_exec_seconds']
        # พิจารณาจากสภาพคล่อง เช่น ไม่เกิน 25 สัญญาต่อออร์เดอร์
        self.pos_size_liquid_limit = self.robot_config['pos_size_liquid_limit']
        # เช่น 0.25 -> 25%
        self.init_reduce_pos_size_percent = self.robot_config['init_reduce_pos_size_percent']

        # Send stop order after open new position immediately if True
        self.send_stop_order_flag = self.robot_config['send_stop_order_flag']

        self.init_robot_positions()
        self.load_trading_state()


    def start(self):
        while True and self.is_trade_time():
            # Get time minute
            minute = int( datetime_util.local_now().strftime('%M') )

            if minute % self.base_bar_interval > 0:
                self.execute_sltp()
            else:
                self.execute()

            # Take a rest ^^
            time.sleep(self.run_interval_sleep)

    # ==================================================================================================
    # BEGIN: Coarse-grained execution helper methods
    # ==================================================================================================
    def build_trade_input(self, **kwargs):
        correl_id = kwargs['correl_id']
        server_time = kwargs['server_time']
        local_time = kwargs['local_time']

        # 1) Load price
        symbol_prices = self.get_price()

        # 2) Parallelize the data loading, 1 process / 1 trading account, 1 trading robot / 1 symbol
        queue = multiprocessing.SimpleQueue()  # Use queue to store returned results
        processes = []
        for account_conf in self.robot_config['accounts']:
            process = multiprocessing.Process(target=self.__get_data_task, args=(correl_id
                                                                           , account_conf['account_no']
                                                                           , queue,))
            processes.append(process)

        for p in processes:
            p.start()

        # NOTE: 1 account / 1 trading robot / 1 list element
        data_list = []
        for _ in processes:
            data = queue.get()  # Get returned results from queue
            data_list.append(data)

        # 3) Call data pipeline to start flow of dataset preparation
        # The 'ml_models' performs like a ML models cache
        class_pipeline = getattr(import_module(self.robot_config['root_module_path'] + '.data_pipeline')
                                 , 'TFEXDataPipeline')
        pipeline = class_pipeline(self.robot_config, symbol_prices, self.ml_models, correl_id=correl_id)
        pipeline.start_flow()
        datasets = pipeline.datasets

        # 4) Set trading input
        trade_input = { 'correl_id' : correl_id
                        , 'timestamp' : timestamp
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
    def __get_data_task(self, correl_id, account_no, queue, **kwargs):
        account_info = self.get_account_info()
        portfolio = self.get_portfolio()
        positions = self.get_position()
        orders = self.get_orders()

        last_bar = prices.iloc[-1]

        # Load account and trading robot configurations
        account_conf = self.__get_account_config(account_no)
        trading_robot_config_list = self.__get_trading_robot_configs(account_info['accountNo'])

        data = {
            'base_time': last_bar['date']
            , 'account_info': account_info
            , 'account_config': account_conf
        }

        # Set specific data for each trading robots under this account
        tr_robots = []
        for tr_robot_config in trading_robot_config_list:
            tr_robot = { 'robot_name' : tr_robot_config['name']
                        , 'config': tr_robot_config
            }

            symbol = self.__get_market_symbol(tr_robot_config['symbol'])
            tr_robot['portfolio'] = portfolio
            tr_robot['positions'] = self.__get_positions_by_symbol(positions, symbol)
            tr_robot['orders'] = self.__get_orders_by_symbol(orders, symbol)
            tr_robot['symbol'] = symbol

            tr_robots.append(tr_robot)

        data['trading_robots'] = tr_robots

        # Put data into queue
        queue.put(data)


    def __handle_action_task(self, trade_input, trade_action, account_info, account_config, trade_data):
        sm = StateMachine

        action_code = trade_action['action_code']

        exec_params = { 'trade_action' : trade_action
                        , 'account_info' : account_info
                        , 'account_config' : account_config
                        , 'trade_input' : trade_input
                        , 'trade_data' : trade_data }

        symbol = trade_data['symbol']
        positions = exec_params['trade_data']['positions']
        orders = exec_params['trade_data']['orders']

        notify_msg = ''

        try:
            if action_code in [sm.ACTION_OPEN_BUY, sm.ACTION_OPEN_SELL]:
                try:
                    # 1) Cancel order(s)
                    if self.__is_position_active_by_symbol(positions, symbol) == True:
                        self.cancel_orders(exec_params=exec_params, symbol=symbol)

                    # 2) Close position
                    self.close_position(exec_params=exec_params)
                    # 3) Wait a seconds
                    time.sleep(self.wait_order_exec_seconds)
                    # 4) Open position
                    self.open_position(exec_params=exec_params)
                    # 5) Wait a seconds
                    time.sleep(self.wait_order_exec_seconds)
                    # 6) Get latest positions and then get active position for this symbol
                    positions = self.get_positions(exec_params, symbol=symbol)
                    active_position = self.__get_active_position_by_symbol(self, positions, symbol)

                    # 7) Send stop order if flag has been set to True
                    if active_position is not None and self.send_stop_order_flag == True:
                        self.send_stop_order(exec_params=exec_params)

                    notify_msg = ''
                except ExecutionError as e:
                    raise ExecutionError('Open new position fail -> {}'.format(e))

            # ==========================================================================================
            elif action_code == sm.ACTION_MODIFY_POSITION:
                # Handle trailing stop by changing stop price and limit price in queuing stop order
                try:
                    # 1) Cancel order(s)
                    if self.__is_active_position_by_symbol(positions, symbol) == True:
                        self.cancel_orders(exec_params=exec_params, symbol=symbol)

                    # 2) Get latest positions and then get active position for this symbol
                    positions = self.get_positions(exec_params, symbol=symbol)
                    active_position = self.__get_active_position_by_symbol(self, positions, symbol)

                    # 3) Send stop order if flag has been set to True
                    if active_position is not None and self.send_stop_order_flag == True:
                        self.send_stop_order(exec_params=exec_params)

                    notify_msg = ''
                except ExecutionError as e:
                    raise ExecutionError('Open new position fail -> {}'.format(e))

            # ==========================================================================================
            elif action_code in [sm.ACTION_CLOSE_BUY, sm.ACTION_CLOSE_SELL]:
                try:
                    # 1) Cancel order(s)
                    if self.__is_active_position_by_symbol(positions, symbol) == True:
                        self.cancel_orders(exec_params=exec_params, symbol=symbol)

                    # 2) Close position
                    self.close_position(exec_params=exec_params)

                    notify_msg = ''
                except ExecutionError as e:
                    raise ExecutionError('Close position fail -> {}'.format(e))

            # ==========================================================================================
            elif action_code in [sm.ACTION_SCALE_OUT_BUY, sm.ACTION_SCALE_OUT_SELL]:
                try:
                    # 1) Get trade type of this position and then check trade type and action code
                    position = self.__get_active_position_by_symbol(positions, symbol)
                    trade_type = self.__get_trade_type(position)
                    if (trade_type in ['BUY', 'LONG'] and action_code == sm.ACTION_SCALE_OUT_BUY) \
                        or (trade_type in ['SELL', 'SHORT'] and action_code == sm.ACTION_SCALE_OUT_SELL):

                        # 2) Check availability of stop order
                        orders = self.__get_orders_by_symbol(orders, symbol)
                        has_stop_order = self.__is_active_stop_order_by_symbol(orders, symbol)

                        # 3) Modify volume in stop order
                        if has_stop_order == True:
                            stop_order = self.__get_active_stop_order_by_symbol(orders, symbol)
                            order_no = stop_order['orderNo']
                            # 4) Calculate new volume after scale out
                            new_volume = stop_order['qty'] - trade_action['volume']
                            self.modify_order(exec_params=exec_params, order_no=order_no, new_volume=new_volume)

                        # 5) Close position
                        self.close_position(exec_params=exec_params)
                        # 6) Wait a seconds
                        time.sleep(self.wait_order_exec_seconds)
                        # 7) Get latest positions and then get active position for this symbol
                        positions = self.get_positions(exec_params, symbol=symbol)
                        active_position = self.__get_active_position_by_symbol(self, positions, symbol)

                        # 8) Send stop order if there is no existing stop order or flag has been set to True
                        if has_stop_order == False and active_position is not None and self.send_stop_order_flag == True:
                            self.send_stop_order(exec_params=exec_params)

                    notify_msg = ''
                except ExecutionError as e:
                    raise ExecutionError('Scale out fail -> {}'.format(e))

            # ==========================================================================================
            elif action_code in [sm.ACTION_SCALE_IN_BUY, sm.ACTION_SCALE_IN_SELL]:
                try:
                    # 1) Get trade type of this position and then check trade type and action code
                    position = self.__get_active_position_by_symbol(positions, symbol)
                    trade_type = self.__get_trade_type(position)
                    if (trade_type in ['BUY', 'LONG'] and action_code == sm.ACTION_SCALE_IN_BUY) \
                            or (trade_type in ['SELL', 'SHORT'] and action_code == sm.ACTION_SCALE_IN_SELL):

                        # 2) Open position
                        self.open_position(exec_params=exec_params)
                        # 3) Wait a seconds
                        time.sleep(self.wait_order_exec_seconds)
                        # 4) Get latest positions and then get active position for this symbol
                        positions = self.get_positions(exec_params, symbol=symbol)
                        active_position = self.__get_active_position_by_symbol(self, positions, symbol)

                        # 5) Check availability of stop order
                        orders = self.__get_orders_by_symbol(orders, symbol)
                        has_stop_order = self.__is_active_stop_order_by_symbol(orders, symbol)

                        # 6) Modify volume in stop order
                        if has_stop_order == True:
                            stop_order = self.__get_active_stop_order_by_symbol(orders, symbol)
                            order_no = stop_order['orderNo']
                            # 7) Calculate new volume after scale in
                            new_volume = stop_order['qty'] + trade_action['volume']
                            self.modify_order(exec_params=exec_params, order_no=order_no, new_volume=new_volume)

                        # 8) Send stop order if there is no existing stop order or flag has been set to True
                        else:
                            if active_position is not None and self.send_stop_order_flag == True:
                                if active_position['actualLongPosition'] > 0:
                                    new_volume = active_position['actualLongPosition']
                                elif active_position['actualShortPosition'] > 0:
                                    new_volume = active_position['actualShortPosition']
                                exec_params['trade_action']['volume'] = new_volume
                                self.send_stop_order(exec_params=exec_params)

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
        #finally:
        #    print('Add code later')
    # ==================================================================================================
    # END: Task methods
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Protected methods
    # ==================================================================================================
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

            trade_type = self.__get_trade_type(settrade_position)
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


    def __get_account_config(self, account_no):
        """
        Get account configuration under account_no
        """
        account_conf = None
        for conf in self.robot_config['accounts']:
            if conf['account_no'] == account_no:
                account_conf = conf
                break
        return account_conf


    def __get_trading_robot_configs(self, account_no):
        """
        Get list of trading robot configurations under account_no
        """
        tr_robot_config_list = []
        for conf in self.robot_config['trading_robots']:
            if conf['account_no'] == account_no:
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


    def __get_trade_type(self, position):
        if position['hasLongPosition'] == True:
            trade_type = 'LONG'
        elif position['hasSHORTPosition'] == True:
            trade_type = 'SHORT'
        return trade_type


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


    def __is_active_stop_order_by_symbol(self, orders, symbol):
        result = False

        if orders is not None:
            for order in orders:
                if order['symbol'] == symbol and self.__is_active_order(order['status']) == True \
                        and order['isStopOrderNotActivate'] == 'Y':
                    result = True
                    break

        return result

    def __get_active_stop_order_by_symbol(self, orders, symbol):
        order = None

        if orders is not None:
            for odr in orders:
                if odr['symbol'] == symbol and self.__is_active_order(odr['status']) == True \
                        and odr['isStopOrderNotActivate'] == 'Y':
                    order = odr
                    break

        return order


    def __get_orders_by_symbol(self, orders, symbol):
        symbol_orders = []

        if orders is not None:
            for order in orders:
                if order['symbol'] == symbol:
                    symbol_orders.append(order)

        return symbol_orders


    def __is_active_position_by_symbol(self, positions, symbol):
        result = False
        if positions is not None:
            for pos in positions:
                if (pos['symbol'] == symbol and (pos['actualLongPosition'] > 0 or pos['actualShortPosition'] > 0)) \
                        or (pos['quantity'] > 0 and pos['symbol_name'] == symbol):
                    result = True
                    break

        return result


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
                if (pos['symbol'] == symbol and (pos['actualLongPosition'] > 0 or pos['actualShortPosition'] > 0))\
                        or (pos['quantity'] > 0 and pos['symbol_name'] == symbol):
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
                            "price": round(action_price, price_decimal_digit),
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
            "price": round(action_price, price_decimal_digit),
            "priceType": price_type,
            "side": side,
            "symbol": symbol,
            "volume": int(volume)
        }
        return request_payload


    def __create_stop_order_req(self, account_conf, trade_action):
        sm = StateMachine

        if trade_action['action_code'] == sm.ACTION_OPEN_BUY:
            side = 'LONG'
        elif trade_action['action_code'] == sm.ACTION_OPEN_SELL:
            side = 'SHORT'

        if 'price_type' in trade_action and trade_action['price_type'] in ['LIMIT', 'ATO', 'MP', 'MP-MTL', 'MP-MKT']:
            price_type = trade_action['price_type']
        else:
            raise Exception('Create stop order request payload fail -> price type in trade action is invalid')

        if 'symbol_name' in trade_action:
            symbol = trade_action['symbol_name']
        else:
            raise Exception('Create stop order request payload fail -> symbol in trade action is invalid')

        if 'action_price' in trade_action:
            action_price = trade_action['action_price']
        else:
            action_price = 0.0

        if 'volume' in trade_action:
            volume = trade_action['volume']
        else:
            raise Exception('Cannot open position -> volume in trade action is invalid')

        if 'stopCondition' in trade_action and 'stopPrice' in trade_action and 'stopSymbol' in trade_action \
                and trade_action['stop_condition'] in ['LAST_PAID_OR_HIGHER', 'LAST_PAID_OR_LOWER' \
                                                       , 'ASK_OR_HIGHER', 'ASK_OR_LOWER' \
                                                       , 'BID_OR_HIGHER', 'BID_OR_LOWER'] \
                and trade_action['stop_price'] > 0.0 and trade_action['stop_symbol'] == trade_action['symbol_name']:
            stop_condition = trade_action['stop_condition']
            stop_price = trade_action['stop_price']
            stop_symbol = trade_action['stop_symbol']
        else:
            raise Exception('Create stop order request payload fail -> stop fields in trade action are invalid')

        request_payload = {
            "pin": account_conf['pin'],
            "position": "OPEN",
            "price": round(action_price, price_decimal_digit),
            "priceType": price_type,
            "side": side,
            "stopCondition": stop_condition,
            "stopPrice": round(stop_price, price_decimal_digit),
            "stopSymbol": stop_symbol,
            "symbol": symbol,
            "volume": int(volume)
        }
        return request_payload


    def __place_order(self, order_type, **kwargs):
        exec_params = kwargs['exec_params']
        account_conf = exec_params['trade_data']['account_conf']
        trade_action = exec_params['trade_action']
        symbol = exec_params['trade_data']['symbol']

        result = False

        retry = 0
        success = False
        while success != True or retry < self.max_retry:
            if order_type == 'CLOSE':
                payload_dict = self.__create_close_order_req(account_conf, trade_action)
            elif order_type == 'OPEN':
                payload_dict = self.__create_open_order_req(account_conf, trade_action)
            elif order_type == 'STOP':
                payload_dict = self.__create_stop_order_req(account_conf, trade_action)

            payload_json = json_util.encode(payload_dict)

            # Place order
            # หาก open position แล้ว fail จะไม่ส่งซ้ำ ยกเลิกการเปิดสถานะไปเลย กันปัญหาจากการดีเลย์
            # แต่ถ้าเป็นการ close position หรือส่ง stop order หาก fail จะวนลูปเพื่อส่งซ้ำ
            order_no == 0
            if order_type in ['CLOSE', 'STOP'] or (order_type == 'OPEN' and retry == 0):
                url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/orders')
                url = url.format(account_conf['broker_id'], account_conf['account_no'])
                order_no = self.__send_http_post_request(url, payload_json)

            # Wait
            time.sleep(self.wait_order_exec_seconds)

            # ==========================================================================================
            # Check sending the close or open action
            if order_type in ['CLOSE', 'OPEN']:
                # Get latest positions and then get active position for this symbol
                positions = self.get_positions(exec_params, symbol=symbol)
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
                    positions = self.get_positions(exec_params, symbol=symbol)
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

            # ======================================================================================
            # Check sending the stop order
            elif order_type == 'STOP':
                # Get latest order
                order = self.get_order_by_id(order_no)
                if order is not None and self.__is_active_order(order['status']) == True:
                    success = True
                    result = True
                else:
                    time.sleep(self.wait_seconds)
                    retry = retry + 1
        # ==============================================================================================
        if (order_type in ['CLOSE', 'STOP'] and retry == self.max_retry and success == False) \
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
        return True


    def backfill_price(self, **kwargs):
        return None


    def gen_correl_id(self, **kwargs):
        return None


    def stamp_finish(self, **kwargs):
        broker_id = xx
        broker_name = xx
        strategy_robot_name = xx
        trading_robot_name = xx
        bar_time = xx
        date_time = xx
        # Update timestamp of latest interval after execution finished
        return None


    def save_trading_state(self, **kwargs):
        return None


    def load_last_trading_state(self, **kwargs):
        """
        example:    [{  'account_no' : account number,
                                'trading_robots' : [{'robot_name', 'trade_type', ...}, ...]
                    }, ...]
        """
        return None


    def build_sub_orders(self, **kwargs):
        order = kwargs['order']

        pos_size_list = self.__build_random_pos_size_list(order['volume'])

        return None


    def get_price(self, account_conf, **kwargs):
        return None


    def get_account_info(self, **kwargs):
        exec_params = kwargs['exec_params']
        account_conf = exec_params['trade_data']['account_conf']

        account_info = None
        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/account-info')
        url = url.format(account_conf['broker_id'], account_conf['account_no'])
        result = self.__send_http_get_request(url)
        if result is not None:
            account_info = json_util.decode(result)
        return account_info


    def get_portfolio(self, **kwargs):
        exec_params = kwargs['exec_params']
        account_conf = exec_params['trade_data']['account_conf']

        portfolio = None
        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/portfolios')
        url = url.format(account_conf['broker_id'], account_conf['account_no'])
        result = self.__send_http_get_request(url)
        if result is not None:
            portfolio = json_util.decode(result)
        return portfolio


    def get_positions(self, **kwargs):
        exec_params = kwargs['exec_params']
        account_conf = exec_params['trade_data']['account_conf']

        if 'symbol' in kwargs:
            symbol = kwargs['symbol']
        else:
            raise Exception("Cannot get positions -> must define 'symbol' in kwargs")

        map_to_internal_position = False
        if 'map_to_internal_position' in kwargs:
            map_to_internal_position = kwargs['map_to_internal_position']

        positions = []
        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/portfolios')
        url = url.format(account_conf['broker_id'], account_conf['account_no'])
        result = self.__send_http_get_request(url)
        if result is not None:
            pos_list = json_util.decode(result)
            if pos_list is not None and len(pos_list) > 0:
                for pos in pos_list:
                    if symbol == None or (symbol is not None and pos['instrument'] == symbol):
                        if map_to_internal_position == True:
                            # Map ฟีลด์ เพื่อเปลี่ยนชื่อและ type ของฟีลด์เป็นแบบที่ระบบภายในใช้
                            pos = self._map_position(pos)
                        positions.append(pos)
        return positions


    def get_trade_by_id(self, **kwargs):
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ไม่ได้ใช้
        return None


    def get_trades(self, **kwargs):
        exec_params = kwargs['exec_params']
        account_conf = exec_params['trade_data']['account_conf']

        trades = None
        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/trades')
        url = url.format(account_conf['broker_id'], account_conf['account_no'])
        result = self.__send_http_get_request(url)
        if result is not None:
            trades = json_util.decode(result)
        return trades

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
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ให้ใช้เมธอด modify_order() แทน
        return False


    def get_order_by_id(self, order_id, **kwargs):
        exec_params = kwargs['exec_params']
        account_conf = exec_params['trade_data']['account_conf']

        order = None
        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/orders/{}')
        url = url.format(account_conf['brokerId'], account_conf['account_no'], order_id)
        result = self.__send_http_get_request(url)
        if result is not None:
            order = json_util.decode(result)
        return order


    def get_orders(self, **kwargs):
        exec_params = kwargs['exec_params']
        account_conf = exec_params['trade_data']['account_conf']

        orders = []
        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/orders')
        url = url.format(account_conf['broker_id'], account_conf['account_no'])
        result = self.__send_http_get_request(url)
        if result is not None:
            orders = json_util.decode(result)
        return orders


    def cancel_order_by_id(self, order_id, **kwargs):
        result = False

        exec_params = kwargs['exec_params']
        account_conf = exec_params['trade_data']['account_conf']

        retry = 0
        cancel_success = False
        while cancel_success != True or retry < self.max_retry:
            payload_dict = { 'pin': account_conf['pin'] }
            payload_json = json_util.encode(payload_dict)

            url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/orders/{}/cancel')
            url = url.format(account_conf['broker_id'], account_conf['account_no'], order_id)
            self.__send_http_patch_request(url, payload_json)

            # Get latest order by order no
            order = self.get_order_by_id(order_id, exec_params=exec_params)
            if order is not None and self.is_active_order(order['status']) == False:
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

        exec_params = kwargs['exec_params']
        account_conf = exec_params['trade_data']['account_conf']
        symbol = exec_params['trade_data']['symbol']
        orders = self.__get_orders_by_symbol(exec_params['trade_data']['orders'], symbol)

        if orders is not None and len(orders) > 0:
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
                while cancel_success != True or retry < self.max_retry:
                    if order_nos_len > 1:
                        payload_dict = { 'orders': order_nos, 'pin': account_conf['pin'] }
                        payload_json = json_util.encode(payload_dict)

                        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/orders/{}/cancel')
                        url = url.format(account_conf['broker_id'], account_conf['account_no'])
                        self.__send_http_patch_request(url, payload_json)

                        # Wait
                        time.sleep(self.wait_order_exec_seconds)

                        # Get latest order list
                        orders = self.get_orders(exec_params=exec_params)
                        if self.__has_active_orders_by_symbol(orders, symbol) == False:
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
        """
        จะแก้ไขออร์เดอร์ในสถานะ queuing หรือ pending กรณี scale out กรณีเดียวเท่านั้น ซึ่งเป็นการแก้ไข volume ใน stop order
        """
        exec_params = kwargs['exec_params']
        account_conf = exec_params['trade_data']['account_conf']
        order = kwargs['order']

        payload_dict = {'newVolume': order['volume'], 'pin': account_conf['pin']}
        payload_json = json_util.encode(payload_dict)

        url = '{}{}'.format(self.base_domain, '/{}/accounts/{}/orders/{}/change')
        url = url.format(account_conf['broker_id'], account_conf['account_no'], order['orderNo'])
        result = self.__send_http_patch_request(url, payload_json)
        return result


    def send_stop_order(self, **kwargs):
        exec_params = kwargs['exec_params']
        order_type = 'STOP'
        result = self.__place_order(order_type, exec_params=exec_params)
        return result


    def cancel_stop_order(self, **kwargs):
        result = False

        exec_params = kwargs['exec_params']

        orders = exec_params['trade_data']['orders']
        if 'symbol' in exec_params['trade_data']:
            symbol = exec_params['trade_data']['symbol']
        else:
            symbol = None

        # Cancel all active orders, including: queueing, pending
        if orders is not None and len(orders) > 0:
            order_nos = []
            for order in orders:
                if self.__is_active_order(order['status']) == True:
                    if symbol == None or (symbol is not None and order['symbol'] == symbol):
                        # Add order no. to be canceled to list
                        order_nos.append(order['orderNo'])

            # Cancel orders
            if len(order_nos) > 0:
                result = self.cancel_orders(exec_params=exec_params, order_nos=order_nos)

        return result
    # ==================================================================================================
    # END: Fine-grained execution helper methods
    # ==================================================================================================
