from deepquant.common.cache_proxy import CacheProxy

import deepquant.common.datetime_util as datetime_util
import deepquant.common.json_util as json_util

import deepquant.data.db_gateway as db_gateway


class TradeMemory():

    def __init__(self, robot_config):
        self.robot_config = robot_config
        self.strategy_name = self.robot_config['strategy_name']
        self.quantity_digits = self.robot_config['quantity_digits']

        self.database_host = self.robot_config['database_host']
        self.database_port = self.robot_config['database_port']
        self.cache = CacheProxy(self.robot_config['cache_host'], self.robot_config['cache_port'])

        self.robot_labels, self.robot_symbol_infos = self.__build_labels_and_symbol_infos()


    def load_trade_pos(self, **kwargs):
        positions = []

        # Get data from cache
        key = 'trade_pos_{}'.format(self.strategy_name)
        cache_data = self.cache.get(key)
        if cache_data is not None:
            positions_temp = json_util.loads(cache_data)
        else:
            # Get data from database
            positions_temp = self._load_trade_pos_from_db()

        if positions_temp is not None and len(positions_temp) > 0:
            positions = positions_temp
        else:
            # This scenario will occur once after deployed.
            blank_positions = self.__build_blank_positions()
            self.save_trade_pos(blank_positions)
            positions = blank_positions

        return positions


    def save_trade_pos(self, trade_positions, **kwargs):
        result = False
        try:
            # 1) Insert one or more positions into database
            self._save_trade_pos_to_db(trade_positions)

            # 2) Load all positions from database
            all_positions = self._load_trade_pos_from_db()

            # 3) Set all positions to cache
            trade_pos_json = json_util.encode(all_positions)
            key = 'trade_pos_{}'.format(self.strategy_name)
            self.cache.set(key, trade_pos_json)


            # 4) Compute and append bar's trading statistics
            self._save_trade_state(trade_positions)

            result = True
        except Exception as e:
            raise Exception('Save trading position(s) error: {}'.format(e))
        return result


    def __get_symbol_id_index(self):
        symbol_id_index_dict = {}
        for i in range(0, len(self.robot_config['symbols'])):
            symbol_id = self.robot_config['symbols'][i]['id']
            symbol_id_index_dict[str(symbol_id)] = i
        return symbol_id_index_dict


    def __build_labels_and_symbol_infos(self):
        robot_labels = {}
        robot_symbol_infos = {}
        symbol_id_index_dict = self.__get_symbol_id_index()
        tr_robot_configs = self.robot_config['trading_robots']
        for conf in tr_robot_configs:
            # Create label (position ID) and save to execution instance
            label = '{}{}{}{}'.format(self.robot_config['strategy_id'], conf['id'], conf['symbol'], conf['trade_model_id'])

            robot_name = conf['name']
            robot_labels[robot_name] = label
            symbol_index = int(symbol_id_index_dict[str(conf['symbol'])])
            robot_symbol_infos[robot_name] = self.robot_config['symbols'][symbol_index]
        return robot_labels, robot_symbol_infos


    def __build_blank_positions(self):
        positions = []
        tr_robot_configs = self.robot_config['trading_robots']
        for conf in tr_robot_configs:
            robot_name = conf['name']
            label = self.robot_labels[robot_name]
            symbol_info = self.robot_symbol_infos[robot_name]
            pos = {'label': label
                   , 'robot_name': robot_name
                   , 'symbol_name': symbol_info['name']
                   , 'trade_type': 'NONE'
                   , 'quantity': 0.0
                   , 'stop_loss': 0.0
                   , 'take_profit': 0.0}
            positions.append(pos)

        return positions


    def _load_trade_pos_from_db(self):
        """Returns list of trading position.
        Each trading position contains: strategy name, robot name, label (magic number),
        symbol name, trade type, entry date, entry price, quantity, stop loss (price), take profit (price)
        """
        positions = []

        try:
            measurement_name = 'trade_pos_{}'.format(self.strategy_name)
            # Query only 1 row for each label
            stmt = 'SELECT * FROM "{}" WHERE label=$label ORDER BY DESC LIMIT 1'.format(measurement_name)

            for trading_robot in list(self.robot_config['trading_robots']):
                robot_name = trading_robot['name']
                label = self.robot_labels[robot_name]
                symbol_info = self.robot_symbol_infos[robot_name]

                bind_params = {'label': label}
                results = db_gateway.query(self.database_host
                                           , self.database_port
                                           , self.robot_config['market'].lower()
                                           , stmt, bind_params=bind_params)
                if results is not None:
                    for point in results:  # 'point' เทียบเท่าคำว่า 'record' ที่ใช้ใน database ทั่วไป
                        position = {'label': point['label']
                            , 'strategy_name': point['strategy_name']
                            , 'robot_name': point['robot_name']
                            , 'symbol_name': point['symbol_name']
                            , 'trade_type': point['trade_type']
                            #, 'entry_date': point['entry_date']
                            #, 'entry_price': round(point['entry_price'], symbol_info['digits'])
                            , 'quantity': int(point['quantity']) if self.quantity_digits == 0 else round(point['quantity'], self.quantity_digits)
                            , 'stop_loss': round(point['stop_loss'], symbol_info['digits'])
                            , 'take_profit': round(point['take_profit'], symbol_info['digits'])}
                        positions.append(position)
        except Exception as e:
            raise Exception('Load trading position(s) from database error: {}'.format(e))
        return positions


    def _save_trade_pos_to_db(self, trade_positions):
        """Save list of trading positions to database.
        Each element in trading positions is dictionary type.
        Each trading position contains: strategy name, robot name, label (magic number),
        symbol name, trade type, entry date, entry price, quantity, stop loss (price), take profit (price)
        """
        if trade_positions is None or len(trade_positions) == 0:
            raise Exception('Save trading position(s) to database error. Trading positions are invalid.')

        try:
            measurement_name = 'trade_pos_{}'.format(self.strategy_name)

            for pos in trade_positions:
                # 1) Set time
                #now = datetime_util.utcnow()
                #timestamp = now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                d_time = datetime_util.localize_bangkok('2016-10-13 15:52:00', '%Y-%m-%d %H:%M:%S')

                # 2) Prepare data
                data = [{
                        "measurement": measurement_name,
                        "tags": {
                            'label': pos['label']
                            , 'strategy_name' : self.strategy_name
                            , 'robot_name': pos['robot_name']
                            , 'symbol_name' : pos['symbol_name']
                        },
                        "fields": {
                            'trade_type': pos['trade_type']
                            #, 'entry_date': pos['entry_date']
                            #, 'entry_price': pos['entry_price']
                            , 'quantity': pos['quantity']
                            , 'stop_loss': pos['stop_loss']
                            , 'take_profit': pos['take_profit']
                        },
                        "time": d_time
                    }]

                # 3) Insert / update trading position
                db_gateway.write_time_series_data(self.database_host
                                                    , self.database_port
                                                    , self.robot_config['market'].lower()
                                                    , data, time_precision='s')

        except Exception as e:
            raise Exception('Save trading position(s) to database error: {}'.format(e))


    def _compute_trade_stats(self, pos):
        """
        :param pos is trading position dictionary
        """
        stats = None
        return stats  # will implement in future version


    def _save_trade_state(self, trade_positions):
        """
        :param pos is trading position dictionary
        :param stats is trading statistics dictionary
        """
        # NOTE: # will implement in future version
        for pos in trade_positions:
            stats = self._compute_trade_stats(pos)

    def save_trade_closed(self, trade_closed, **kwargs):
        result = False
        try:
            # 1) Insert one or more positions into database
            self._save_trade_closed_to_db(trade_closed)

            # 4) Compute and append bar's trading statistics
            #self._save_trade_closed_to_db(trade_closed)

            result = True
        except Exception as e:
            raise Exception('Save trading position(s) error: {}'.format(e))
        return result

    def _save_trade_closed_to_db(self, trade_closed):
        """Save list of trading close position to database.
        Each element in trading close position is dictionary type.
        Each trading closed position contains: date time, account no, robot name, label (magic number), balance, profit/loss
        """
        if trade_closed is None or len(trade_closed) == 0:
            raise Exception('Save trading closed position(s) to database error. Trading positions are invalid.')

        try:
            
            if trade_closed['order_history_info']['details'] :
                order_detail = trade_closed['order_history_info']['details']
                for key in order_detail.keys() :

                    measurement_name = 'closed_trade_pos_{}_{}'.format(self.strategy_name, order_detail[key]['robot_name'])

                    # 1) Set time
                    now = datetime_util.utcnow()
                    timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
                    d_time = datetime_util.localize_bangkok( timestamp, '%Y-%m-%d %H:%M:%S')

                    # 2) Prepare data
                    data = [{
                            "measurement": measurement_name,
                            "tags": {
                                'label': order_detail[key]['label']
                                , 'strategy_name' : self.strategy_name
                                , 'robot_name': order_detail[key]['robot_name']
                                , 'symbol_name' : order_detail[key]['symbol']
                            },
                            "fields": {
                                'datetime': d_time
                                , 'droplet_ip': trade_closed['order_history_info']['droplet_ip']
                                , 'account_no': trade_closed['order_history_info']['account_no']
                                , 'label': order_detail[key]['label']
                                , 'order_no': order_detail[key]['order_no']
                                , 'entry_time': order_detail[key]['open_time']
                                , 'entry_price': order_detail[key]['open_price']
                                , 'symbol': order_detail[key]['symbol']
                                , 'trade_type': order_detail[key]['trade_type']
                                , 'quantity': order_detail[key]['lot']
                                , 'exit_time': order_detail[key]['close_time']
                                , 'exit_price': order_detail[key]['close_price']
                                , 'profit': order_detail[key]['profit']
                                , 'equity': trade_closed['order_history_info']['equity']
                            },
                            "time": d_time
                        }]

                    # 3) Insert / update trading position
                    db_gateway.write_time_series_data(self.database_host
                                                        , self.database_port
                                                        , self.robot_config['market'].lower()
                                                        , data, time_precision='s')

        except Exception as e:
            raise Exception('Save trading position(s) to database error: {}'.format(e))