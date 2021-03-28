import multiprocessing
import time
import os
import urllib
import pandas as pd
import numpy as np

import deepquant.common.datetime_util as datetime_util
import deepquant.common.line_notify as line_notify
import deepquant.common.json_util as json_util
import deepquant.common.cache_proxy as cache_proxy
from deepquant.common.state_machine import StateMachine

import deepquant.data.db_gateway as db_gateway

from deepquant.ems.base_exec import BaseExec
from deepquant.ems.error import ExecutionError


"""
Instruction:
- สร้าง environment variable ชื่อ TK_OANDA เพื่อเก็บ path ที่เก็บ token ที่ generate จากเว็บ OANDA

==============================================================================================
OANDA schema:

- N position / 1 symbol หมายถึง หลาย position เทรด symbol เดียวกันได้
- รายละเอียดของ position ที่ระบบภายในเราใช้ ไปตรงกับที่เก็บใน trade ของ OANDA หมายถึง ชื่อคำศัพท์ไม่ตรงกัน ดังนั้นอย่างง ^^
- OANDA V20 API 1 position / N trade หมายถึง 1 position มีได้หลาย trade ซึ่งแต่ละ trade จะเป็น long หรือ short ก็ได้
  แต่ระบบเรากำหนดเป็น 1 position / 1 trade แล้วถ้าอยากถือสถานะหลายๆ เทรด ก็ใช้ trading robot หลายๆ ตัว
  เพราะ 1 trading robot / 1 position / 1 trade เช่น มี 5 trading robot ก็จะถือสถานะได้ 5 position
  แบบนี้จะจัดการเรื่อง modularity ของโค้ดและกลยุทธ์เทรดได้ดีกว่า ยืดหยุ่นกว่า
- เมื่อ map ให้เข้ากับระบบภายในเรา จะเป็น 1 trade / 1 label ซึ่ง label มีความหมายเทียบเท่า trade ID หรือ magic number ใน MetaTrader

ข้อกำหนดสำคัญสำหรับระบบเราในเวอร์ชั่นนี้คือ
1. 1 strategy robot / 1 account หมายถึง strategy robot 1 ชุดสามารถเทรดได้กับ account number เดียวเท่านั้น
(OANDA V20 API ใช้คำว่า account ID)

2. 1 position / 1 trade / 1 label / 1 trading robot
แต่ 1 strategy robot มีได้หลาย trading robot นะ
หมายความว่า strategy robot 1 ชุด สามารถมีได้หลาย trading robot และได้หลาย trade นั่นเอง
เราใช้การแบ่งแบบนี้ เพื่อให้ง่ายต่อการจัดการโครงสร้างโรบอท

3. OANDA V2 API มีฟีลด์ที่ชื่อว่า clientExtensions ประกอบด้วย id, tag และ comment ซึ่งคำว่า label เทียบเท่ากับ id ในที่นี้
ดังนั้นการอ้างอิง เช่น การโหลด order, trade จะใช้ค่า label แทนค่าใน clientExtensions['id'] ในการค้นหาและดึงข้อมูล
เช่น label = 123 ก็จะค้นหา trade ที่มี clientExtensions['id'] = 123 ในการค้นหาและดึงข้อมูล trade

4. การเตรียม trade input เพื่อส่งเข้าไปในระบบภายในตั้งแต่ strategy endpoint -> strategy robot ใช้เกณฑ์ดังนี้
4.1. จะโหลดทุก trade ภายใต้ account จาก OANDA V20 API
4.2 เก็บทุก trade ลงใน dictionary key ชื่อ positions เพราะ 1 trade ต่อ 1 position อยู่แล้ว
และเพราะระบบภายในไม่มีใช้คำว่า trade แต่ใช้คำว่า position
4.3 เมื่อระบบเริ่มรันในรอบนั้นๆ จะเริ่มต้นโหลด account info โดยเรียกใช้เมธอดชื่อ get_account_info()
ซึ่งภายในเป็นการเรียก OANDA V20 API endpoint นี้ -> /v3/accounts/<account_number>
ซึ่ง output ที่ได้กลับมาจะมีฟีลด์ต่างๆ ของ account *รวมถึง ทุก trade, ทุก position และทุก order ภายใต้ account
เป็นการโหลดข้อมูลแบบ eager load (หาอ่านได้ใน Eager Acquisition Architectural Pattern [POSA3]
ทำให้จะได้ output data แบบ coarse-grained granularity เป็นก้อนใหญ่กลับมาเลย
ข้อดีคือช่วยเพิ่ม stability ในการทำงานลำดับถัดๆ ไป และลด latency ระหว่างการทำงาน

5. จากข้อ 3 ข้อมูลที่โหลดมาจะเก็บใส่ dictionary key ชื่อ oanda_account ดังนั้นหากต้องการเข้าถึงข้อมูลแบบละเอียดสมบูรณ์จาก OANDA
ให้ใช้จากในนี้แทน
*ซึ่งชื่อฟีลด์ต่างๆ มีฟอร์แมตแตกต่างจากที่ใช้ในระบบเรา ชื่อฟีลด์ของ OANDA V20 API ขึ้นต้นด้วยตัวเล็กคั่นคำด้วยอักษรตัวใหญ่ เช่น clientExtensions
ชื่อฟีลด์ในระบบเราใช้อักษรตัวเล็กหมด คั่นคำด้วย '_' เช่น account_number

==============================================================================================
Cache (Redis wrapper) schema:
- key 'pos_<label>', value คือ trade action dictionary {label, robot_name, trade_type, quantity, stop_loss, take_profit}
"""


class OandaExec(BaseExec):

    def __init__(self, robot_config):
        super().__init__(robot_config)

        base_token = open(os.environ[self.base_token_env_var]).read().strip()
        self.http_header = { 'Authorization' : 'Bearer {}'.format(base_token)
                             , 'Content-Type' : 'application/json' }

        self.symbol_load_count = 2 # Load แค่ 2 bar ล่าสุด
        self.init_robot_positions()


    def start(self):
        while True:
            if (self.robot_config['enable_check_trade_time'] == True and self.is_trade_time())\
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

                if minute % self.base_bar_interval > 0:
                    if self.latest_prices != self.prev_exec_sltp_prices:
                        # 1) Execute in-bar
                        self.execute_sltp()
                        # 2) Memorize latest executed prices
                        self.prev_exec_sltp_prices = self.latest_prices
                else:
                    # 1) Generate new correlation ID
                    correl_id = self.gen_correl_id()
                    if self.prev_correl_id != correl_id and self.latest_prices != self.prev_exec_prices:
                        self.cur_correl_id = correl_id

                        # 2) Backfill automatically
                        if self.require_backfill():
                            self.backfill_price()

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
    def handle_trade_actions(self, **kwargs):
        trade_input = kwargs['trade_input']
        trade_actions = kwargs['trade_actions']

        if trade_actions is not None and len(trade_actions) > 0:
            # ========================================================================================
            # ========================================================================================
            # Parallelize the trading action handling, 1 process / 1 trading action / 1 trading robot
            # ในแต่ละรอบการทำงาน แต่ละ trading robot จะมี output แค่ 1 action เท่านั้น
            if self.config['run_mode'] == 'live':
                processes = []
                for action in trade_actions:
                    process = multiprocessing.Process(target=self.__handle_action_task, args=(trade_input, action,))
                    processes.append(process)

                for p in processes:
                    p.start()
            # ========================================================================================
            # ========================================================================================
            elif self.config['run_mode'] == 'debug':
                for action in trade_actions:
                    self.__handle_action_task(trade_input, action)

        return trade_input['correl_id']
    # ==================================================================================================
    # END: Coarse-grained execution helper methods
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Task methods
    # ==================================================================================================
    def __handle_action_task(self, trade_input, trade_action):
        sm = StateMachine

        # Initialize cache proxy (a redis wrapper). One connection / trade action handling task
        cache = cache_proxy.CacheProxy(self.robot_config['cache_host'], self.robot_config['cache_port'])

        robot_name = trade_action['robot_name']
        action_code = trade_action['action_code']
        symbol_info = trade_input['symbol_infos'][robot_name]
        exec_params = { 'trade_action' : trade_action
                        , 'trade_input' : trade_input
                        , 'symbol_info' : symbol_info
                        , 'cache' : cache }

        # อย่างง ในระบบภายในเราใช้คำว่า position แต่ตรงกับคำว่า trade ใน OANDA V20 API
        # ชื่อฟีลด์และ type ได้ปรับให้ตรงกับที่ระบบภายในใช้กับ position แล้ว
        # เป็น dictionary, key คือ robot name ดึงจาก trade_action['robot_name']
        position = trade_input['positions'][robot_name]

        notify_msg = ''

        try:
            if action_code in [sm.ACTION_OPEN_BUY, sm.ACTION_OPEN_SELL, sm.ACTION_MODIFY_POSITION
                                , sm.ACTION_CLOSE_BUY, sm.AcTION_CLOSE_SELL
                                , sm.ACTION_SCALE_OUT_BUY, sm.ACTION_SCALE_OUT_SELL]:

                notify_msg = super()._handle_action_task_common(trade_action, position, exec_params)

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
        symbol_prices= {}

        for symbol_tf in symbol_tf_list:
            s = symbol_tf.split(self.symbol_tf_delimiter)
            symbol = s[0]
            tf = s[1]

            # Get price from OANDA V20 API
            price_df = self.get_price(symbol=symbol, timeframe=tf, count=self.symbol_load_count
                                      , include_incomplete_candle=False)
            symbol_prices[symbol_tf] = price_df.to_json(orient='records')

        # เก็บราคาที่โหลดมาล่าสุดไว้ใน object
        self.latest_prices = symbol_prices

        return symbol_prices


    def _get_active_position(self, positions, label=None, symbol=None, **kwargs):
        trade = None
        trades = positions
        if trades is not None:
            for tr in trades:
                if (tr['currentUnits'] > 0 and tr['clientExtensions']['id'] == label)\
                        or (tr['quantity'] > 0 and tr['label'] == label):
                    trade = tr
                    break
        return trade


    def _map_account(self, account_number, original_account, **kwargs):
        account_info = {}
        oanda_account = original_account
        account_info['acc_number'] = account_number
        account_info['currency'] = float(oanda_account['currency'])
        account_info['balance'] = float(oanda_account['balance'])
        account_info['unrealized_pl'] = float(oanda_account['unrealizedPL'])
        account_info['pl'] = float(oanda_account['pl'])
        account_info['financing'] = float(oanda_account['financing'])
        account_info['commission'] = float(oanda_account['commission'])
        account_info['margin_rate'] = float(oanda_account['marginRate'])
        account_info['margin_used'] = float(oanda_account['marginUsed'])
        account_info['margin_available'] = float(oanda_account['marginAvailable'])
        return account_info


    def _map_position(self, original_position, symbol_tick_size, **kwargs):
        position = None

        oanda_trade = original_position

        if oanda_trade is not None:
            position = {}

            position['id'] = oanda_trade['id']
            position['label'] = oanda_trade['clientExtensions']['id']
            position['symbol_name'] = oanda_trade['instrument']

            initial_units = float(oanda_trade['initialUnits'])
            if initial_units > 0:
                position['trade_type'] = 'BUY'
            elif initial_units < 0:
                position['trade_type'] = 'SELL'

            position['entry_time'] = oanda_trade['openTime']

            open_price = float(oanda_trade['price'])
            position['entry_price'] = open_price
            position['quantity'] = float(oanda_trade['currentUnits'])

            stop_loss = 0.0
            take_profit = 0.0
            if oanda_trade['stopLossOrder'] is not None:
                stop_loss = float(oanda_trade['stopLossOrder']['price'])
                position['stop_loss'] = stop_loss
            else:
                position['stop_loss'] = 0.0

            if oanda_trade['takeProfitOrder'] is not None:
                position['take_profit'] = float(oanda_trade['takeProfitOrder']['price'])
            else:
                position['take_profit'] = 0.0

            if position['trade_type'] == 'BUY':
                if stop_loss > 0:
                    position['stop_loss_pips'] = round( (open_price - stop_loss) / (symbol_tick_size * 10), 2 )
                else:
                    position['stop_loss_pips'] = 0.0

                if take_profit > 0:
                    position['take_profit_pips'] = round( (take_profit - open_price) / (symbol_tick_size * 10), 2 )
                else:
                    position['take_profit_pips'] = 0.0

            elif position['trade_type'] == 'SELL':
                if stop_loss > 0:
                    position['stop_loss_pips'] = round( (stop_loss - open_price) / (symbol_tick_size * 10), 2 )
                else:
                    position['stop_loss_pips'] = 0.0

                if take_profit > 0:
                    position['take_profit_pips'] = round( (open_price - take_profit) / (symbol_tick_size * 10), 2 )
                else:
                    position['take_profit_pips'] = 0.0


            position['gross_profit'] = float(oanda_trade['unrealizedPL'])
            position['net_profit'] = float(oanda_trade['realizedPL'])
            position['trans_cost'] = float(oanda_trade['financing'])

            # 10 USD / 1 pip
            position['profit_pips'] = position['gross_profit'] / position['quantity'] / 10.0

        return position


    def _get_latest_position(self, exec_params, symbol_tick_size, **kwargs):
        position = self._map_position(self.get_trade_by_id(exec_params=exec_params), symbol_tick_size)
        return position
    # ==================================================================================================
    # END: Protected methods
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Private methods
    # ==================================================================================================
    def __get_trade_type(self, trade):
        if float(trade['initialUnits']) > 0:
            trade_type = 'LONG'
        elif float(trade['initialUnits']) < 0:
            trade_type = 'SHORT'
        return trade_type


    def __is_active_order(self, order_status):
        if order_status.lower() in ['pending']:
            return True
        else:
            return False


    def __has_active_orders_by_label(self, orders, label):
        result = False
        if orders is not None:
            for order in orders:
                if order['clientExtensions']['id'] == label and self.__is_active_order(order['state']) == True:
                    result = True
                    break
        return result


    def __get_order_by_label(self, orders, label):
        order = None
        if orders is not None:
            for ord in orders:
                if ord['clientExtensions']['id'] == label:
                    order = ord
                    break
        return order


    def __create_market_order(self, trade_action):
        if trade_action['trade_type'] == 'BUY':
            units = str( round(trade_action['quantity'], self.quantity_digits) )
        elif trade_action['trade_type'] == 'SELL':
            units = str( round(-1 * trade_action['quantity'], self.quantity_digits) )

        mk_order = {
                        'type' : 'MARKET',
                        'instrument' : trade_action['symbol_name'],
                        'units' : units,
                        'timeInForce' : 'FOK',
                        'positionFill' : 'DEFAULT',
                        'tradeClientExtensions' : {'id' : trade_action['label']}
        }

        if 'stop_loss' in trade_action and trade_action['stop_loss'] > 0.0:
            mk_order['stopLossOnFill'] = { 'price' : trade_action['stop_loss'] }

        if 'take_profit' in trade_action and trade_action['take_profit'] > 0.0:
            mk_order['takeProfitOnFill'] = { 'price' : trade_action['take_profit'] }

        return mk_order


    def __adjust_prices(self, price_candles, price_type):
        """
        Returns price DataFrame. Columns: datetime, open, high, low, close, volume
        """

        if price_type == 'M':
            price = 'mid'
        elif price_type == 'B':
            price = 'bid'
        elif price_type == 'A':
            price = 'ask'

        df = pd.DataFrame(price_candles)

        df1 = pd.DataFrame(0.0, index=np.arange(len(price_candles)), columns=['datetime'])
        df2 = pd.DataFrame('', index=np.arange(len(price_candles)), columns=['o', 'h', 'l', 'c'])
        df3 = pd.DataFrame(0, index=np.arange(len(price_candles)), columns=['volume'])

        df['time'] = df['time'].str.replace('.000000000Z', '')
        df['time'] = df['time'].str.replace(':', '').str.replace('T', '').str.replace('-', '')

        df1['datetime'] = df['time']
        df3['volume'] = df['volume']

        for i in range(0, df1.shape[0]):
            df2.iloc[i] = price_candles[i][price]

        df2 = df2.astype(float)
        df2.columns = ['open', 'high', 'low', 'close']

        final_df = pd.concat([df1, df2, df3], axis=1)

        return final_df


    def __get_symbol_info(self, symbol_name):
        """
        Returns a dictionary of symbol info
        :return: a dictionary of symbol info
        """
        symbol_info = None
        for symbol in self.robot_config['symbols']:
            if symbol['name'] == symbol_name:
                symbol_info = symbol
                break
        return symbol_info
    # ==================================================================================================
    # END: Private methods
    # ==================================================================================================


    # ==================================================================================================
    # BEGIN: Fine-grained execution helper methods
    # ==================================================================================================
    def is_trade_time(self, **kwargs):
        return True


    def is_market_open(self, **kwargs):
        return True


    def backfill_price(self, **kwargs):
        """
        backfill ราคาย้อนหลังได้สูงสุดแค่ 5,000 แท่ง เท่านั้น หรือประมาณ 20 กว่าวัน
        ดังนั้นไม่ควรปิดบอทหยุดเทรดเกิน 20 วัน หากเกินหรือไม่แน่ใจ ต้อง backfill ราคาแบบ historical data ก่อน
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
                    # Get price from OANDA V20 API
                    price_df = self.get_price(symbol=symbol, timeframe=tf, count=self.backfill_price_count)
                    price_json = price_df.to_json(orient='records')

                    # Backfill
                    result = db_gateway.backfill(db_host, db_port, market, broker_id, symbol, tf, price_json)

                    # เก็บ <symbol>|<TF> ลงลิสต์
                    backfill_already_list.append(symbol_tf)
        except Exception as e:
            raise ExecutionError('Backfill price fail for {}|{} -> {}'.format(symbol, tf, e))

        return result


    def get_price(self, **kwargs):
        # Example date time format: 2016-01-01T00:00:00.000000000Z

        if 'symbol' in kwargs:
            instrument = kwargs['symbol']
        else:
            raise Exception("Cannot get prices -> must define 'symbol' in kwargs")

        param = '?'

        if 'timeframe' in kwargs:
            param = '{}granularity={}'.format(param, kwargs['timeframe'].upper())
        else:
            raise Exception("Cannot get prices -> must define 'timeframe' in kwargs")

        if 'count' in kwargs and kwargs['count'] > 0 and kwargs['count'] <= 5000:
            param = '{}&count={}'.format(param, int(kwargs['count']))
        else:
            raise Exception("Cannot get prices -> invalid 'count', allow count range is range 1 - 5,000")

        if 'price_type' in kwargs and kwargs['price_type'].upper() in ['B', 'M', 'A']:
            price_type = kwargs['price_type'].upper()
            param = '{}&price={}'.format(param, price_type)
        else:
            price_type = 'M'
            param = '{}&price=M'.format(param)

        if 'dt_from' in kwargs:
            param = '{}&from={}'.format(param, urllib.parse.quote(kwargs['dt_from']))

        if 'dt_to' in kwargs:
            param = '{}&to={}'.format(param, urllib.parse.quote(kwargs['dt_to']))

        prices = []
        url = '{}{}'.format(self.base_domain, '/v3/instruments/{}/candles{}')
        url = url.format(instrument, param)
        response = self.send_http_get_request(url, self.http_header)

        if response is not None and response.status_code == 200:
            initial_prices = json_util.decode(response.text)
            if initial_prices['candles'] is not None and len(initial_prices['candles']) > 0:
                if 'include_incomplete_candle' in kwargs and kwargs['include_incomplete_candle'] == False:
                    total_candles = len(initial_prices['candles'])
                    if initial_prices['candles'][total_candles - 1]['complete'] == False:
                        # Remove candle สุดท้ายที่ยังไม่ปิด ระบบ execution นี้ใช้ราคาจาก candle ก่อนหน้าที่ปิดแล้ว
                        initial_prices['candles'] = initial_prices[0:total_candles - 1]
                prices = self.__adjust_prices(initial_prices['candles'], price_type)
        return prices


    def get_account_info(self, **kwargs):
        """
        Output schema อ้างอิงได้ในลิงค์ http://developer.oanda.com/rest-live-v20/account-df/#collapse_definition_2
        โดยดูที่ Account definition ซึ่งใน Account ประกอบด้วยฟีลด์จำนวนมาก รวมถึง open positions, open trades, pending orders ด้วย
        """
        exec_params = kwargs['exec_params']
        account_number = exec_params['trade_input']['account']['acc_number']

        url = '{}{}'.format(self.base_domain, '/v3/accounts/{}')
        url = url.format(account_number)
        response = self.send_http_get_request(url)
        if response is not None and response.status_code == 200:
            account_info = json_util.decode(response.text)['account']
        else:
            raise ExecutionError("Get account info fail -> Has no returned result from API")
        return account_info


    def get_portfolio(self, **kwargs):
        # ไม่มีการอิมพลีเม้นต์เมธอดนี้ ให้ใช้ get_account_info() แทน
        # เพราะ Account API ของ OANDA สามารถดึงทีเดียวได้ทั้ง account, positions, trades
        return None


    def get_positions(self, **kwargs):
        positions = self.get_trades(map_to_internal_position=True, **kwargs)
        return positions


    def get_trade_by_id(self, **kwargs):
        """
        Returns trade ที่มีค่า id ตรงตามที่กำหนดในอาร์กิวเม้นต์ label
        *NOTE: คำว่า id ในะรบเราคือคำว่า label
        """
        exec_params = kwargs['exec_params']
        trade_action = exec_params['trade_action']
        account_number = exec_params['trade_input']['account']['acc_number']

        # label คือ trade ID ที่กำหนดฟอร์แมตและระบุโดยผู้ใช้ เหมือนกับ magic number ใน MetaTrader
        label = trade_action['label']
        symbol_tick_size = float(exec_params['symbol_info']['tick_size'])

        trade = None
        url = '{}{}'.format(self.base_domain, '/v3/accounts/{}/trades/@{}')
        url = url.format(account_number, label)
        response = self.send_http_get_request(url)
        if response is not None and response.status_code == 200:
            trade = json_util.decode(response.text)['trade']
            # Map ฟีลด์ เพื่อเปลี่ยนชื่อและ type ของฟีลด์เป็นแบบที่ระบบภายในใช้
            trade = self._map_position(trade, symbol_tick_size)
        return trade


    def get_trades(self, **kwargs):
        """
        Return open trades (OANDA V20 API) (หรือคำว่า positions ในระบบเรา)
        ซึ่ง return ทุก trade ที่เทรด instrument (symbol) เดียวกัน
        """
        exec_params = kwargs['exec_params']
        account_number = exec_params['trade_input']['account']['acc_number']
        symbol_tick_size = float(exec_params['symbol_info']['tick_size'])

        map_to_internal_position = False
        if 'map_to_internal_position' in kwargs:
            map_to_internal_position = kwargs['map_to_internal_position']

        symbol = None
        if 'symbol' in kwargs:
            symbol = kwargs['symbol']

        trades = []
        url = '{}{}'.format(self.base_domain, '/v3/accounts/{}/openTrades')
        url = url.format(account_number)
        response = self.send_http_get_request(url)

        if response is not None and response.status_code == 200:
            trade_obj = json_util.decode(response.text)
            if trade_obj is not None and 'trades' in trade_obj and len(trade_obj['trades']) > 0:
                trade_list = trade_obj['trades']
                for trade in trade_list:
                    if symbol == None or (symbol is not None and trade['instrument'] == symbol):
                        if map_to_internal_position == True:
                            # Map ฟีลด์ เพื่อเปลี่ยนชื่อและ type ของฟีลด์เป็นแบบที่ระบบภายในใช้
                            trade = self._map_position(trade, symbol_tick_size)
                        trades.append(trade)
        return trades


    def close_trade(self, **kwargs):
        result = False

        if 'exec_params' in kwargs:
            exec_params = kwargs['exec_params']
            account_number = exec_params['trade_input']['account']['acc_number']
            trade_action = exec_params['trade_action']
            # label คือ trade ID ที่กำหนดฟอร์แมตและระบุโดยผู้ใช้ เหมือนกับ magic number ใน MetaTrader
            label = trade_action['label']

            if 'quantity' in trade_action and trade_action['quantity'] != 0:
                quantity = round(trade_action['quantity'], self.quantity_digits)
            else:
                raise Exception("Cannot close trade -> must define 'quantity' in trade action")
        else:
            account_number = self.robot_config['account_number']
            label = kwargs['label']
            quantity = round(kwargs['quantity'], self.quantity_digits)

        response = None
        retry = 0
        close_success = False
        while close_success != True and retry < self.max_retry:
            payload_dict = {'units': '{}'.format(quantity)}
            payload_json = json_util.encode(payload_dict)

            url = '{}{}'.format(self.base_domain, '/v3/accounts/{}/trades/@{}/close')
            url = url.format(account_number, label)
            response = self.send_http_put_request(url, payload_json)

            # Validate response
            if response is not None and response.status_code == 200:
                close_success = True
                result = True
            else:
                time.sleep(self.wait_order_exec_seconds)
                retry = retry + 1

        if retry == self.max_retry and close_success == False:
            if response is not None and response.status_code != 200:
                error_msg_dict = json_util.decode(response.text)
                raise ExecutionError('Close trade fail -> {}'.format(error_msg_dict['errorMessage']))
        return result


    def modify_trade(self, **kwargs):
        result = False

        exec_params = kwargs['exec_params']
        account_number = exec_params['trade_input']['account']['acc_number']
        trade_action = exec_params['trade_action']
        symbol_info = exec_params['symbol_info']

        # label คือ trade ID ที่กำหนดฟอร์แมตและระบุโดยผู้ใช้ เหมือนกับ magic number ใน MetaTrader
        if 'label' in kwargs:
            label = kwargs['label']
        else:
            raise Exception("Modify trade fail -> must define 'label' in kwargs")

        # Set stop loss and take profit
        payload_dict = {}
        if 'stop_loss' in trade_action and trade_action['stop_loss'] > 0:
            sl = str( round(trade_action['stop_loss'], int(symbol_info['digits'])) )
            payload_dict['stopLoss'] = { 'price' : sl
                , 'clientExtensions' : { 'id' : str(label), 'tag' : self.robot_config['strategy_name'] } }

        if 'take_profit' in trade_action and trade_action['take_profit'] > 0:
            tp = str( round(trade_action['take_profit'], int(symbol_info['digits'])) )
            payload_dict['takeProfit'] = { 'price' : tp
                , 'clientExtensions' : { 'id' : str(label), 'tag' : self.robot_config['strategy_name'] } }

        response = None
        retry = 0
        modify_success = False
        while modify_success != True and retry < self.max_retry:
            payload_json = json_util.encode(payload_dict)

            url = '{}{}'.format(self.base_domain, '/v3/accounts/{}/trades/@{}/orders')
            url = url.format(account_number, label)
            response = self.send_http_put_request(url, payload_json)

            # Validate response
            if response is not None and response.status_code == 200:
                modify_success = True
                result = True
            else:
                time.sleep(self.wait_order_exec_seconds)
                retry = retry + 1

        if retry == self.max_retry and modify_success == False:
            if response is not None and response.status_code != 200:
                error_msg_dict = json_util.decode(response.text)
                raise ExecutionError('Modify trade fail -> {}'.format(error_msg_dict['errorMessage']))
        return result


    def open_position(self, **kwargs):
        exec_params = kwargs['exec_params']
        account_number = exec_params['trade_input']['account']['acc_number']
        trade_action = exec_params['trade_action']

        try:
            payload_dict = self.__create_market_order(trade_action)
            payload_json = json_util.encode(payload_dict)

            # Place order
            # หาก open position แล้ว fail จะไม่ส่งซ้ำ ยกเลิกการเปิดสถานะไปเลย กันปัญหาจากการดีเลย์
            # แต่ถ้าเป็นการ close position หรือส่ง stop order หาก fail จะวนลูปเพื่อส่งซ้ำ
            url = '{}{}'.format(self.base_domain, '/v3/accounts/{}/orders')
            url = url.format(account_number)
            response = self.send_http_post_request(url, payload_json)

            # Validate response
            if response is not None and response.status_code != 201:
                result = True
            else:
                # ไม่ลอง open position ใหม่ ยกเลิกไปเลย แล้ว notify แจ้ง error แทน
                error_msg = json_util.decode(response.text)
                raise ExecutionError('Send order OK but response fail -> {}'.format(error_msg['errorMessage']))

        except ExecutionError as e:
            raise ExecutionError('Open new position (trade) fail -> {}'.format(e))
        return result


    def close_position(self, **kwargs):
        # Forward ไปเรียก close_trade() แทน
        result = self.close_trade(**kwargs)
        return result


    def modify_position(self, **kwargs):
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ใช้การ modify trade แทน
        return False


    def get_order_by_id(self, order_id, **kwargs):
        """
        Returns order ที่มีค่า id ตรงตามที่กำหนดในอาร์กิวเม้นต์ label
        *NOTE: คำว่า id ในะรบเราคือคำว่า label
        """
        exec_params = kwargs['exec_params']
        account_number = exec_params['trade_input']['account']['acc_number']

        # label คือ trade ID ที่กำหนดฟอร์แมตและระบุโดยผู้ใช้ เหมือนกับ magic number ใน MetaTrader
        if 'label' in kwargs:
            label = kwargs['label']
        else:
            raise Exception("Get order by ID fail -> must define 'label' in kwargs")

        order = None
        url = '{}{}'.format(self.base_domain, '/v3/accounts/{}/orders/@{}')
        url = url.format(account_number, label)
        response = self.send_http_get_request(url)
        if response is not None and response.status_code == 200:
            order = json_util.decode(response.text)['order']
        return order


    def get_orders(self, **kwargs):
        """
        Returns list of 'pending' order ภายใต้ account นี้
        """
        exec_params = kwargs['exec_params']
        account_number = exec_params['trade_input']['account']['acc_number']

        orders = []
        url = '{}{}'.format(self.base_domain, '/v3/accounts/{}/pendingOrders')
        url = url.format(account_number)
        response = self.send_http_get_request(url)
        if response is not None and response.status_code == 200:
            orders = json_util.decode(response.text)['orders']
        return orders


    def cancel_order_by_id(self, order_id, **kwargs):
        result = False

        exec_params = kwargs['exec_params']
        account_number = exec_params['trade_input']['account']['acc_number']

        # label คือ trade ID ที่กำหนดฟอร์แมตและระบุโดยผู้ใช้ เหมือนกับ magic number ใน MetaTrader
        if 'label' in kwargs:
            label = kwargs['label']
        else:
            raise Exception("Cancel order by ID fail -> must define 'label' in kwargs")

        response = None
        retry = 0
        cancel_success = False
        while cancel_success != True and retry < self.max_retry:
            url = '{}{}'.format(self.base_domain, '/v3/accounts/{}/orders/@{}/cancel')
            url = url.format(account_number, label)
            response = self.send_http_put_request(url)

            # Evaluate response
            if response.status_code == 200:
                cancel_success = True
                result = True
            else:
                time.sleep(self.wait_order_exec_seconds)
                retry = retry + 1

        if retry == self.max_retry and cancel_success == False:
            if response is not None and response.status_code != 200:
                error_msg_dict = json_util.decode(response.text)
                raise ExecutionError('Cancel order by ID fail -> {}'.format(error_msg_dict['errorMessage']))
        return result


    def cancel_orders(self, **kwargs):
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ไม่ได้ใช้
        return None


    def modify_order(self, **kwargs):
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ใช้การ modify trade แทน
        return None


    def cancel_stop_order(self, **kwargs):
        # ไม่ได้อิมพลีเม้นต์เมธอดนี้ ไม่ได้ใช้
        return None
    # ==================================================================================================
    # END: Fine-grained execution helper methods
    # ==================================================================================================
