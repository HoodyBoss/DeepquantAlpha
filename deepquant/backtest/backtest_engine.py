import pandas as pd
import logging
import copy
import yaml
import sys
import time
import multiprocessing

import os
from distutils.dir_util import copy_tree, mkpath

import importlib.util
from importlib import import_module

import deepquant.common.state_machine as state_machine
import deepquant.common.datetime_util as datetime_util
import deepquant.robotemplate_fx.robot_context as robot_ctx

# create logger
logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

"""
## Trading Position

position คือ สถานะการเทรดของแต่ละ label โดย 1 position คือ 1 สถานะการเทรด จะมีจำนวนน้อยกว่าจำนวน bar เสมอ

แต่ละ position มี correlation id คือ label ใน MetaTrader เรียกว่า magic number)
และ position ที่มี label เดียวกันจะถือสถานะของสินค้าได้แค่ 1 symbol เท่านั้น
  
### Trading Position Attributes
position ประกอบด้วย attribute ต่างๆ ได้แก่ trade_id, symbol, trade_type, exit_reason, entry_date (ใช้ bar time),
entry_price, exit_date, exit_price, price_change, price_change_pct, pos_size, pos_value, stop_loss, take_profit, 
stop_loss_price, take_profit_price, profit_points, unrealized_profit, unrealized_profit_pct, net_profit,
net_profit_pct, hold_bars, scale_in_num, scale_out_num

### NOTE:
1. ค่า stop loss กับ take profit ใช้เป็นค่า price ไม่ใช่ pip การคำนวณจาก pip เป็น price จะถูกทำตอน execute order ก่อนหน้านี้
2. การ backtest จะมี 2 โหมด ได้แก่ โหมดเร็ว กับโหมดละเอียด โดยโหมดเร็วจะบันทึกเฉพาะ trade position จัดเก็บเป็น trade list
ส่วนโหมดละเอียดจะบันทึก position ในทุก bar เพื่อเอาไว้วิเคราะห์เทรดแบบละเอียดยิบ
3. แอททริบิวต์ที่โหมดเร็วไม่บันทึก แต่จะบันทึกในโหมดละเอียดได้แก่ stop_loss_price, take_profit_price, comment, gross_profit

### Using Message Broker in Detailed Mode
ในโหมดละเอียด จะทำการบันทึกค่ารายละเอียด trade position ของแต่ละ bar ทุก bar ดังนั้นในประมวลจะมีรูปแบบดังนี้
1. thread หลักจากคำนวณรายละเอียด trade position ก่อน
2. ส่งรายละเอียดเป็นรูปแบบ message ส่งไปที่ message broker ในการทำงานแบบ asynchronous
โดย thread หลักก็ประมวลผลขั้นตอนถัดไปได้ทันทีโดยไม่ต้องรอ
3. message broker รับ message ที่มีรายละเอียด trade position ของ bar นั้น แล้วส่งต่อให้กับ message consumer นำไปจัดเก็บ

### Trading Position Rules
* ระบบจะสร้าง position เมื่อมี order action คือ open buy หรือ open sell
* position เป็น mutable dictionary object อยู่ในหน่วยความจำ แต่ละ label จะมีแค่ 1 dictionary instance ณ เวลาใดเวลาหนึ่งเท่านั้น
สำหรับ position ที่เปลี่ยนไปในแต่ละ bar จะจัดเก็บแยกต่างหากใน DataFrame ชื่อ detailed_trades
และ position ที่ exit แล้วก็จะจัดเก็บแยกต่างหากเช่นกัน ใน DataFrame ชื่อ trades
* position จะถูกประมวลผลในทุก bar เพื่อจัดการกับ modify action และเพื่อตรวจสอบ SL, TP และอัพเดตสถานะของ position
เช่น gross profit, netprofit, ขนาดกำไร (pip) ที่เปลี่ยนไป
* order action ที่เกิดขึ้นบน active position ได้แก่ modify กับ close
* กรณีใน bar นั้นเกิด modify action จะสามารถ modify ได้แค่ SL หรือ TP เท่านั้น
* การ close มี 2 แบบ คือ close position กับ partial close ซึ่งก็คือการทำ scale out นั่นเอง หมายถึงการ close เพียงบางส่วน
โดยต้องระบุ quantity (position size) ที่จะ close ด้วย
* position ที่มีสถานะ active ยังไม่ได้ exit เทรด แอททริบิวต์ exited จะมีค่าเท่ากับศูนย์
* position ที่ exit เทรดแล้ว แอททริบิวต์ exited จะมีค่ามากกว่าศูนย์ ขึ้นกับเหตุผลในการ exit
* ความหมายของค่าแอททริบิวต์ exited: 0 = active trade 1 = exited trade
* ความหมายของค่าแอททริบิวต์ exit_reason: 1 = exited by close action, 2 = exited by SL, 3 = exited by TP,
4 = exited by trailing stop, 5 = exited by time
"""

class MockRobotContext(robot_ctx.BaseRobotContext):
    def __init__(self, config):
        super().__init__(config)


class ScaleList():

    def __init__(self):
        self.SCALE_TYPE_IN = 1
        self.SCALE_TYPE_OUT = 2
        self.scale_list = []
        self.total_scale_outs = 0
        self.total_scale_ins = 0

    def append(self, bar_index, date, scale_type, scale_size \
               , pos_size_before, pos_size_after \
               , avg_cost_before, avg_cost_after):
        scale_no = 0
        if scale_type == self.SCALE_TYPE_IN:
            scale_no = self.total_scale_ins + 1
        elif scale_type == self.SCALE_TYPE_OUT:
            scale_no = self.total_scale_outs + 1

        track = {'scale_no': scale_no
            , 'bar_index': bar_index
            , 'date': date
            , 'scale_type': scale_type
            , 'scale_size': scale_size
            , 'pos_size_before': pos_size_before
            , 'pos_size_after': pos_size_after
            , 'avg_cost_before': avg_cost_before
            , 'avg_cost_after': avg_cost_after}
        self.scale_list.append(track)

    def get_list(self):
        return self.scale_list


class Trade():

    def __init__(self, trade_id, symbol, trade_type, entry_date, entry_price
                 , pos_size, stop_loss=0.0, take_profit=0.0
                 , stop_loss_price=0.0, take_profit_price=0.0):
        self.trade_id = trade_id
        self.symbol = symbol
        self.trade_type = trade_type
        self.entry_date = entry_date
        self.entry_price = entry_price
        self.pos_size = pos_size
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.stop_loss_price = stop_loss_price
        self.take_profit_price = take_profit_price

        self.scale_list = ScaleList()

        self.avg_cost = 0.0
        self.exit_date = None
        self.exit_price = 0.0
        self.exit_reason = 0

        self.price_change = 0.0
        self.price_change_pct = 0.0
        self.pos_value = 0.0

        self.profit_points = 0.0
        self.unrealized_profit = 0.0
        self.unrealized_profit_pct = 0.0
        self.net_profit = 0.0
        self.net_profit_pct = 0.0
        self.hold_bars = 0.0
        # self.scale_in_num = 0
        # self.scale_out_num = 0


class BacktestEngine():

    def __init__(self, config):
        self.config = config

        # List of dictionary, dictionary item: 'name', 'bars'
        # Bar dict contains datetime, open, high, low, close, volume, open_buy, open_sell
        # close_buy, close_sell, stop_loss, take_profit, pos_size, scale_type
        # scale_size, stop_type
        self.model_bars = []

        self.idx_datetime = 0
        self.idx_price_open = 1
        self.idx_price_high = 2
        self.idx_price_low = 3
        self.idx_price_close = 4
        self.idx_price_volume = 5
        self.idx_open_buy = 6
        self.idx_open_sell = 7
        self.idx_close_buy = 8
        self.idx_close_sell = 9
        self.idx_stop_loss = 10
        self.idx_take_profit = 11
        self.idx_pos_size = 12
        self.idx_scale_type = 13
        self.idx_scale_size = 14
        self.idx_stop_type = 15

        # Load price dataframe
        self.price_datasets = []
        for price_file in self.config['price_files']:
            df = pd.read_csv('{}/{}'.format(self.config['root_price_path'], price_file))
            self.price_datasets.append(df)

        # self.config['trade_model_path']
        robot_config_file_path = '{}{}'.format(self.config['robot_config_root_path'], self.config['robot_config_file'])
        self.robot_config = yaml.safe_load(open(robot_config_file_path))
        self.robot_config_mod_time = self.__get_last_mod_time(robot_config_file_path)
        # True = if config has been modified before running, False = if config has not been modified
        self.robot_config_has_modified = self.__check_modified(robot_config_file_path)

        # Load and create object of trading models
        self.trade_models = self.__build_trade_models()

        # Reset flag
        # True = จะรัน signal processing ใหม่
        # False = จะเช็กไฟล์ signal result ก่อน หากไม่มีการเปลี่ยนแปลง จะโหลดผล signal ของเก่าที่เคยรันก่อนหน้า
        # แต่ถ้ามีการเปลี่ยนแปลง จะรัน signal processing ใหม่อีกครั้ง
        self.reset_flag = False

    # =======================================================================================
    # BEGIN: Public methods
    # =======================================================================================
    def reset(self):
        self.reset_flag = True

    def backtest(self, mode=2, start_date=None, end_date=None):
        """
        Run backtest. If you want to run some range of all bars, must define both start_date and end_date.

        param: mode,  1 = run signal processing only, 2 = run both signal processing and full backtest
        param: start_date, start date time to run signal processing and/or full backtest
        param: end_date, end date time to run signal processing and/or full backtest
        """
        logger.info('%s [%s]: %s', datetime_util.bangkok_now(), 'INFO'
                    , "Started...")

        # If mode is run signal processing only
        if mode == 1:
            # Run signal processing
            self.process_signal()


        # If mode is run both signal processing and full backtest
        elif mode == 2:
            # Run signal processing
            self.process_signal()

            # Run backtest
            self.__process_backtest()

        logger.info('%s [%s]: %s', datetime_util.bangkok_now(), 'INFO'
                    , "Finished...")

    def optimize(self):
        print('Wait a minute please ^^')

    def process_signal(self):
        logger.info('%s [%s]: %s', datetime_util.bangkok_now(), 'INFO'
                    , "Signal processing started...")

        queue = multiprocessing.SimpleQueue()  # Use queue to store returned results
        processes = []
        model_index = 0
        for robot in list(self.robot_config['trading_robots']):
            robot_name = robot['name']
            process = multiprocessing.Process(target=self.__handle_process_signal, args=(model_index, robot_name, queue,))
            processes.append(process)
            model_index = model_index + 1

        for p in processes:
            p.start()

        for _ in processes:
            model_bars_dict = queue.get()  # Get returned results from queue
            self.model_bars.append(model_bars_dict)

        # for bar_dict in self.model_bars:
        #    print(bar_dict['bars'])

        logger.info('%s [%s]: %s', datetime_util.bangkok_now(), 'INFO'
                    , "Signal processing finished...")

    # =======================================================================================
    # END: Public methods
    # =======================================================================================

    def __build_trade_models(self):
        trade_models = []
        trade_model_root_path = '{}{}/'.format(self.config['trade_model_root_path']
                                               , self.robot_config['root_module_path'])
        for i in range(0, len(self.robot_config['trading_robots'])):
            trading_robot_config = self.robot_config['trading_robots'][i]

            trade_model_module_path = (trade_model_root_path + trading_robot_config['trade_model_module']).replace('.', '/')

            # Set datasets
            # Note: trading model 1 ตัวสามารถมี dataset ได้มากกว่า 1 dataset
            # เช่น dataset ตัวนึงเอาไว้สำหรับ predict open buy/sell dataset อีกตัวเอาไว้สำหรับ predict SL/TP/Pos.size
            price_datasets_index = 0 if len(self.config['price_files']) == 1 else i
            datasets = {}
            for model_name in trading_robot_config['model_names']:
                # Will check deepcopy in future in case of performance optimization
                datasets[model_name] = copy.deepcopy(self.price_datasets[price_datasets_index])

            # Initialize mock robot context
            robot_context = MockRobotContext(self.robot_config)
            robot_context.datasets = datasets

            tr_model_id = trading_robot_config['trade_model_id']
            tr_model_class = trading_robot_config['trade_model_class']

            # Get symbol name
            symbol_name = self.__get_symbol_name(self.robot_config, trading_robot_config['symbol'])

            # Create trade model object using dynamic import
            spec = importlib.util.spec_from_file_location(trading_robot_config['trade_model_module'], trade_model_module_path + '.py')
            model_module = importlib.util.module_from_spec(spec)
            sys.modules[trading_robot_config['trade_model_module']] = model_module
            spec.loader.exec_module(model_module)
            model_module_class = getattr(model_module, trading_robot_config['trade_model_class'])
            trade_model = model_module_class(symbol_name \
                                             , robot_context \
                                             , self.robot_config \
                                             , trading_robot_config['trade_model_id'])
            trade_models.append(trade_model)
            # print(trade_models[0].trade_model_name)
            # print(trade_models[0].datasets[trading_robot_config['name']])
        return trade_models

    def __handle_process_signal(self, model_index, robot_name, queue):
        """
        robot_name คือ trading robot name ซึ่งเป็นชื่อเดียวกันกับ trading model name
        *ใน config ของ trading robot แต่ละตัวมีฟีลด์ชื่อ model_names ตรงนี้หมายถึง predictive model
        ซึ่งไม่ได้ถูกเรียกโดยตรงตอน backtest
        """
        """
        file_path = config['root_file_path'] + 'fx_barracuda_gold_ami5_signal2.csv'
        sig_df = pd.read_csv(file_path)
        bars = sig_df.to_numpy()
        model_bars_dict = {'name':robot_name, 'bars':bars}
        queue.put(model_bars_dict)
        """

        # Prepare path
        price_datasets_index = 0 if len(self.config['price_files']) == 1 else model_index
        price_file = self.config['price_files'][price_datasets_index].replace('.csv', '').replace('.txt', '')
        output_file = '{}_{}.csv'.format(robot_name, price_file)
        cur_path = os.path.abspath(os.getcwd())
        sig_results_path = '{}/{}'.format(cur_path, 'signal_results')
        mkpath(sig_results_path)

        sig_result_file_path = '{}/{}'.format(sig_results_path, output_file)
        sig_file_has_modified = False
        sig_file_not_found = False
        try:
            sig_file_has_modified = self.__check_modified(sig_result_file_path)
        except FileNotFoundError:
            sig_file_not_found = True

        # Predict new one or load from existing file
        sig_df = None
        if (sig_file_has_modified == False and sig_file_not_found == False and self.robot_config_has_modified == False) \
                and self.reset_flag == False:
            sig_df = pd.read_csv('{}/{}'.format(sig_results_path, output_file))

        elif not (sig_file_has_modified == False and sig_file_not_found == False and self.robot_config_has_modified == False) \
                or self.reset_flag == True:
            # Get trading model and call prediction
            trade_model = self.trade_models[model_index]
            output_type = 1  # MUST BE 1
            sig_df = trade_model.predict(output_type=output_type)

            # Write signals (with prices) to file
            sig_df.to_csv(sig_result_file_path, index=False)
            mod_time_since_epoc = os.path.getmtime(sig_result_file_path)

            # Update running timestamp
            track_file_path = '{}/{}_{}_{}.txt'.format(sig_results_path, robot_name, price_file, 'ts')
            track_file = open(track_file_path, 'w')
            track_file.write(str(mod_time_since_epoc))
            track_file.close()

        # Get numpy array from dataframe and put into queue for returning to backtest engine object
        model_bars_dict = None
        if sig_df is not None:
            bars = sig_df.to_numpy()
            model_bars_dict = {'name': robot_name, 'bars': bars}
        queue.put(model_bars_dict)

    def __get_last_mod_time(self, file_path):
        # Get epoc time of last modified time of specified file
        mod_time_since_epoc = os.path.getmtime(file_path)
        return mod_time_since_epoc

    def __check_modified(self, file_path):
        has_modified = False

        track_file_path = ''
        fs = file_path.split('.')
        for i in range(0, len(fs) - 1):
            track_file_path = track_file_path + fs[i]
        track_file_path = '{}_ts.txt'.format(track_file_path)

        # Check last modified time
        track_file = None
        track_file_found = False
        try:
            track_file = open(track_file_path, 'r')
            track_file_found = True
        except FileNotFoundError:
            track_file = open(track_file_path, 'w')
            track_file.write('')
            track_file.close()
            track_file = open(track_file_path, 'r')

        prev_mod_time_since_epoc = 0.0
        track_data = track_file.read()
        if track_data != '':
            prev_mod_time_since_epoc = float(track_data)
        track_file.close()

        mod_time_since_epoc = 0.0
        try:
            mod_time_since_epoc = self.__get_last_mod_time(file_path)
            if track_file_found == False or mod_time_since_epoc != prev_mod_time_since_epoc:
                track_file = open(track_file_path, 'w')
                track_file.write(str(mod_time_since_epoc))
                track_file.close()
        except FileNotFoundError:
            raise

        # Determine
        if mod_time_since_epoc != prev_mod_time_since_epoc:
            has_modified = True

        return has_modified

    def __process_backtest(self):
        logger.info('%s [%s]: %s', datetime_util.bangkok_now(), 'INFO'
                    , "Backtest started...")

        sm = state_machine.StateMachine()
        tr_robot_configs = self.robot_config['trading_robots']

        total_bar = 0
        try:
            total_bar = self.model_bars[0]['bars'].shape[0]
            print('total_bar = {}'.format(total_bar))
        except:
            raise Exception('Could not get total bar')

        # Initialize shared balance and equity between models
        balance = self.config['balance']
        equity = balance

        # Initialization of each models
        model_cur_trades = []
        model_cur_trade_ids = []
        model_ids = []
        for i in range(0, len(tr_robot_configs)):
            model_cur_trades.append(None)
            model_cur_trade_ids.append(0)
            model_ids.append(i + 1)

        # ===================================================================================
        # Loop through all bars
        for i in range(0, total_bar):
            for j in range(0, len(tr_robot_configs)):
                model_bar_dict = self.__get_model_bar_dict(tr_robot_configs[j]['name'])
                model_name = model_bar_dict['name']
                bar = model_bar_dict['bars'][i]
                if i > 0:
                    prev_bar = self.model_bars[j]['bars'][i - 1]
                else:
                    prev_bar = None

                action = self.__handle_process_bar(balance
                                                   , equity
                                                   , model_ids[j]
                                                   , model_cur_trade_ids[j]
                                                   , model_cur_trades[j]
                                                   , bar, prev_bar)
                if action == 1:
                    print('datetime:{}, action:{}'.format(bar[self.idx_datetime]
                                                          , sm.get_action_name(action)))
        # ===================================================================================
        logger.info('%s [%s]: %s', datetime_util.bangkok_now(), 'INFO'
                    , "Backtest finished...")

    def __handle_process_bar(self, balance, equity, model_id, cur_trade_id
                             , cur_trade, bar, prev_bar):
        sm = state_machine.StateMachine()

        # Get basic action code: open buy / open sell / close buy / close sell
        action = self.__get_basic_action(bar, cur_trade, sm)

        # 1=close price on signal bar, 2=open price of next bar
        action_bar = self.config['action_bar_price']
        action_date, action_price = self.__get_action_dateprice(action_bar, bar, prev_bar)

        # ===================================================================================
        if action in [sm.ACTION_OPEN_BUY, sm.ACTION_OPEN_SELL]:
            # Handle enter trade
            self.__handle_enter_trade(model_id, cur_trade_id, cur_trade
                                      , bar, prev_bar, action_date, action_price)
        elif action in [sm.ACTION_CLOSE_BUY, sm.ACTION_CLOSE_SELL]:
            # Handle exit trade
            exit_reason = 0
            self.__handle_exit_trade(cur_trade, action_date, action_price, exit_reason)
        elif action in [sm.ACTION_HOLD_BUY, sm.ACTION_HOLD_SELL]:
            # Handle stop loss and take profit
            self.__handle_sltp(bar, prev_bar, cur_trade, action_date)

            # If trade is still alive
            # Handle action on hold position state
            if cur_trade.exit == 0:
                self.__handle_action_on_position()

        # ===================================================================================

        # Update trading statistics
        self.__update_trade_stats()

        return action

    def __update_trade_stats(self):
        print('Hello')

    def __handle_enter_trade(self, model_id, cur_trade_id, cur_trade
                             , bar, prev_bar, action_date, action_price):
        new_trade_id = self.__generate_trade_id(model_id, cur_trade_id)

        cur_trade = Trade(new_trade_id, symbol, trade_type, entry_date, entry_price
                          , pos_size, stop_loss=0.0, take_profit=0.0
                          , stop_loss_price=0.0, take_profit_price=0.0)

    def __generate_trade_id(self, model_id, cur_trade_id):
        return int(str(model_id) + str(cur_trade_id + 1))

    def __handle_exit_trade(self, cur_trade, action_date, action_price, exit_reason):
        cur_trade.exit_date = action_date
        cur_trade.exit_price = self.__simulate_exit_price(cur_trade.trade_type
                                                          , cur_trade.pos_size
                                                          , action_price)
        cur_trade.exit_reason = exit_reason

    def __handle_sltp(self, bar, prev_bar, cur_trade, action_date):
        reached_sl = False
        reached_tp = False
        reached_price = 0.0

        if cur_trade != None:
            # Get current trading position's attributes
            trade_type = cur_trade.trade_type
            sl_price = cur_trade.stop_loss_price
            tp_price = cur_trade.take_profit_price

            # 1=close price on signal bar, 2=open price of next bar
            action_bar = self.config['action_bar_price']
            if action_bar == 1:
                price_open = bar[self.idx_price_open]
                price_high = bar[self.idx_price_high]
                price_low = bar[self.idx_price_low]

            elif action_bar == 2:
                price_open = prev_bar[self.idx_price_open]
                price_high = prev_bar[self.idx_price_high]
                price_low = prev_bar[self.idx_price_low]

            if trade_type == state_machine.STATE_BUY:
                # Check stop loss
                if price_open <= sl_price:
                    reached_sl = True
                    reached_price = price_open
                elif price_low <= sl_price:
                    reached_sl = True
                    reached_price = sl_price

                # Check take profit
                if price_open >= tp_price:
                    reached_tp = True
                    reached_price = price_open
                elif price_high >= tp_price:
                    reached_tp = True
                    reached_price = _price

            elif trade_type == state_machine.STATE_SELL:
                # Check stop loss
                if price_open >= sl_price:
                    reached_sl = True
                    reached_price = price_open
                elif price_high >= sl_price:
                    reached_sl = True
                    reached_price = sl_price

                # Check take profit
                if price_open <= tp_price:
                    reached_tp = True
                    reached_price = price_open
                elif price_low <= tp_price:
                    reached_tp = True
                    reached_price = tp_price

            # Determine exit trade
            if reached_price > 0:
                if action == sm.ACTION_HOLD_BUY:
                    action = sm.ACTION_CLOSE_BUY
                elif action == sm.ACTION_HOLD_SELL:
                    action = sm.ACTION_CLOSE_SELL

                exit_reason = 2 if reached_sl == True else 3
                self.__handle_exit_trade(cur_trade, action_date, reached_price, exit_reason)

    def __handle_action_on_position(self):
        print('Hello')

    def __get_basic_action(self, bar_state_row, cur_trade, st_machine):
        action = st_machine.ACTION_WAIT

        # Determine trade type
        # NOTE: trade type and trade state has same meaning
        cur_trade_type = state_machine.State.STATE_IDLE
        if cur_trade is not None:
            cur_trade_type = cur_trade.trade_type  # 1=Buy, 2=Sell, 3=IDLE

        # Get signal and set action
        if bar_state_row[self.idx_close_buy] == 1 and bar_state_row[self.idx_open_sell] == 0:
            action = st_machine.ACTION_CLOSE_BUY
        elif bar_state_row[self.idx_close_sell] == 1 and bar_state_row[self.idx_open_buy] == 0:
            action = st_machine.ACTION_CLOSE_SELL
        elif bar_state_row[self.idx_open_buy] == 1:
            action = st_machine.get_trade_action(st_machine.SIGNAL_BUY, cur_trade_type)
        elif bar_state_row[self.idx_open_sell] == 1:
            action = st_machine.get_trade_action(st_machine.SIGNAL_SELL, cur_trade_type)

        return action

    def __simulate_exit_price(self, trade_type, pos_size, action_price):
        exit_price = 0.0
        slippage = self.__simulate_slippage(pos_size)

        if trade_type == state_machine.STATE_BUY:
            exit_price = action_price - slippage
        elif trade_type == state_machine.STATE_SELL:
            exit_price = action_price + slippage

        return exit_price

    def __get_action_dateprice(self, action_bar, bar, prev_bar):
        if action_bar == 1:
            action_date = bar[self.idx_datetime]
            action_price = bar[self.idx_price_close]
        elif action_bar == 2:
            action_date = prev_bar[self.idx_datetime]
            action_price = prev_bar[self.idx_price_open]

        return action_date, action_price

    def __simulate_slippage(self, pos_size):
        slippage = 0.0

        # Add code later

        return slippage

    def __get_symbol_name(self, robot_config, symbol_id):
        symbol_name = None
        for symbol in robot_config['symbols']:
            if symbol['id'] == symbol_id:
                symbol_name = symbol['name']
                break
        return symbol_name

    def __get_model_bar_dict(self, model_name):
        model_bar_dict = None
        for mbd in self.model_bars:
            if mbd['name'] == model_name:
                model_bar_dict = mbd
                break
        return model_bar_dict

