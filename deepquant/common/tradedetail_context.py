from flask import current_app
from flask_redis import FlaskRedis
import pandas as pd

import deepquant.common.state_machine as state_machine

try:
   from six.moves import cPickle as pickle
except:
   import pickle

# This is a wrapper class for trading details log, encapsulating session using Redis
class TradeDetailContext :

    column_list = None

    # A constructor of TradeDetailContext
    # Param: robot_name - a robot name
    def __init__(self, robot_name, robot_config):
        self.cache_key = robot_name + "_trade_detail"
        self.db = FlaskRedis(current_app)
        self.config = robot_config
        self.__st_machine = state_machine.StateMachine()
        self.__state = state_machine.State()
        self.column_list = ['datetime', 'signal_code', 'action_code', 'equity', 'avg_cost', 'market_price' \
            , 'pos_size', 'pos_value', 'stop_loss', 'stop_loss_price', 'move_stop_loss_datetime' \
            , 'profit_point', 'profit_percent', 'profit_money', 'num_of_scale_in', 'num_of_scale_out' \
            , 'scale_datetime', 'scale_size']

    #===================================================================================================================
    # BEGIN: Common accessing methods
    #===================================================================================================================
    def get_last_row(self):
        """
        Return last row  of trade details log containing all columns

        Returns:
            Dataframe
        """
        df = self.get()

        return df[-1:]

    def get(self):
        """
        Return all rows of data in session
        """
        return pickle.loads(self.db.get(self.cache_key))

    def set(self, data):
        """
        Store data into session
        """
        self.db.set(self.cache_key, pickle.dumps(data, pickle.HIGHEST_PROTOCOL))

    def drop_first_row(self):
        """
        Delete first row in data
        """
        df = self.get()
        df = df.drop(df.index[[0]])
        self.set(df)

    def append_row(self, new_row_df):
        """
        Append new row to existing data
        """
        df = self.get()
        df = df.append(new_row_df)
        df.index = pd.to_datetime(df.index, utc=True)
        df.index = df.index.tz_convert(new_row_df.index.tz)

        self.set(df)
    #===================================================================================================================
    # END: Common accessing methods
    #===================================================================================================================

    #===================================================================================================================
    # BEGIN: Utility methods
    #===================================================================================================================
    def get_field(self, field_name):
        field_value = self.get_last_row()[field_name]
        return field_value
    #===================================================================================================================
    # END: Utility methods
    #===================================================================================================================

    #===================================================================================================================
    # BEGIN: Private methods
    #===================================================================================================================
    def __is_open_action(self, action_code):
        result = False

        if action_code == self.__st_machine.ACTION_OPEN_BUY \
                or action_code == self.__st_machine.ACTION_OPEN_SELL:
            result = True

        return result

    def __is_close_action(self, action_code):
        result = False

        if action_code == self.__st_machine.ACTION_CLOSE_BUY \
                or action_code == self.__st_machine.ACTION_CLOSE_SELL \
                or action_code == self.__st_machine.ACTION_CLOSE_BUY_WAIT \
                or action_code == self.__st_machine.ACTION_CLOSE_SELL_WAIT \
                or action_code == self.__st_machine.ACTION_CLOSE_BUY_WAIT_FOR_BUY \
                or action_code == self.__st_machine.ACTION_CLOSE_BUY_WAIT_FOR_SELL \
                or action_code == self.__st_machine.ACTION_CLOSE_SELL_WAIT_FOR_SELL \
                or action_code == self.__st_machine.ACTION_CLOSE_SELL_WAIT_FOR_BUY:
            result = True

        return result

    def __is_hold_action(self, action_code):
        result = False

        if action_code == self.__st_machine.ACTION_HOLD_BUY \
                or action_code == self.__st_machine.ACTION_HOLD_SELL:
            result = True

        return result

    def __is_wait_action(self, action_code):
        result = False

        if action_code == self.__st_machine.ACTION_WAIT \
                or action_code == self.__st_machine.ACTION_WAIT_FOR_BUY \
                or action_code == self.__st_machine.ACTION_WAIT_FOR_SELL:
            result = True

        return result

    def __is_scale_action(self, action_code):
        result = False

        if action_code == self.__st_machine.ACTION_SCALE_IN_BUY \
                or action_code == self.__st_machine.ACTION_SCALE_OUT_BUY \
                or action_code == self.__st_machine.ACTION_SCALE_IN_SELL \
                or action_code == self.__st_machine.ACTION_SCALE_OUT_SELL:
            result = True

        return result

    def __cal_stop_loss_price(self, trade_action, cur_position, entry_price):
        stop_loss = 0.0 # price or index
        stop_loss_point = trade_action.stop_loss

        if self.__is_wait_action(trade_action.action_code) == False:
            if self.__st_machine.decode_state(cur_position) == self.__state.STATE_BUY:
                stop_loss = entry_price - stop_loss_point
            elif self.st_machine.decode_state(cur_position) == self.__state.STATE_SELL:
                stop_loss = entry_price + stop_loss_point

        return stop_loss

    def __cal_move_stop_loss_datetime(self, trade_action):
        datetime = None

        if self.__is_wait_action(trade_action.action_code) == False:
            if self.get_last_row()['stop_loss'] != trade_action.stop_loss:
                datetime = trade_action.datetime

        return datetime

    def __cal_profit(self, cur_position, avg_cost, position_size, market_price):
        profit = list() #0=profit_point, 1=profit_percentage, 2=profit_money

        profit_point = 0.0
        profit_percent = 0.0
        profit_money = 0.0

        if position_size > 0:
            point_value = self.config['point_value']
            position_value = avg_cost * point_value * position_size
            market_value = market_price * point_value * position_size

            if self.__st_machine.decode_state(cur_position) == self.__state.STATE_BUY:
                profit_point = market_price - avg_cost
                profit_money = market_value - position_value
            elif self.__st_machine.decode_state(cur_position) == self.__state.STATE_SELL:
                profit_point = avg_cost - market_price
                profit_money = position_value - market_value

                profit_percent = (profit_money * 100) / position_value

        profit.append(round(profit_point, 2))
        profit.append(round(profit_percent, 2))
        profit.append(round(profit_money, 2))

        return profit

    def __get_move_stop_loss_datetime(self, trade_action):
        datetime = None
        if self.__is_hold_action(trade_action.action_code) \
                or self.__is_close_action(trade_action.action_code) \
                or self.__is_scale_action(trade_action.action_code):

            if trade_action.stop_loss != self.get_last_row()['stop_loss']:
                datetime = trade_action.datetime

        return datetime

    def __build_data_case1(self, trade_input, trade_action):
        portfolio_entry = trade_input.cur_trade_portfolio_entry

        datetime = trade_action.datetime
        signal_code = trade_action.signal_code
        action_code = trade_action.action_code
        equity = trade_input.account_info['equity']

        cur_position = portfolio_entry['position']
        avg_cost = portfolio_entry['avg_cost']
        market_price = portfolio_entry['market_price']
        pos_size = portfolio_entry['actual_pos_size']
        pos_value = portfolio_entry['amount']

        entry_price = avg_cost
        stop_loss = trade_action.stop_loss
        # Stop loss in trade action is point but the following is price
        stop_loss_price = self.__cal_stop_loss_price(trade_action, cur_position, entry_price)
        move_stop_loss_datetime = self.__get_move_stop_loss_datetime(trade_action)

        profit_point = 0.0
        profit_percent = 0.0
        profit_money = 0.0
        num_of_scale_in = 0
        num_of_scale_out = 0
        scale_datetime = None
        scale_size = 0.0

        data = [datetime, signal_code, action_code, equity, avg_cost, market_price, pos_size, pos_value \
            , stop_loss, stop_loss_price, move_stop_loss_datetime, profit_point, profit_percent \
            , profit_money, num_of_scale_in, num_of_scale_out, scale_datetime, scale_size]

        return data

    def __build_data_case2(self,trade_input, trade_action, cur_trade):
        portfolio_entry = trade_input.cur_trade_portfolio_entry

        datetime = trade_action.datetime
        signal_code = trade_action.signal_code
        action_code = trade_action.action_code
        equity = trade_input.account_info['equity']

        cur_position = portfolio_entry['position']
        avg_cost = portfolio_entry['avg_cost']
        market_price = portfolio_entry['market_price']
        pos_size = portfolio_entry['actual_pos_size']
        pos_value = portfolio_entry['amount']

        entry_price = 0.0
        if self.__is_open_action(self.get_last_row()['action_code']):
            entry_price = avg_cost
        else:
            entry_price = cur_trade.get_entry_price()

        stop_loss = trade_action.stop_loss
        # Stop loss in trade action is point but the following is price
        stop_loss_price = self.__cal_stop_loss_price(trade_action, cur_position, entry_price)
        move_stop_loss_datetime = self.__get_move_stop_loss_datetime(trade_action)

        profit_point = portfolio_entry['market_price'] - avg_cost
        profit_percent = portfolio_entry['unrealized_profit_percent']
        profit_money = portfolio_entry['unrealized_profit']
        num_of_scale_in = 0
        num_of_scale_out = 0
        scale_datetime = None
        scale_size = 0.0

        data = [datetime, signal_code, action_code, equity, avg_cost, market_price, pos_size, pos_value \
            , stop_loss, stop_loss_price, move_stop_loss_datetime, profit_point, profit_percent \
            , profit_money, num_of_scale_in, num_of_scale_out, scale_datetime, scale_size]

        return data

    def __build_data_case3(self, trade_input, trade_action):
        datetime = trade_action.datetime
        signal_code = trade_action.signal_code
        action_code = trade_action.action_code
        equity = trade_input.account_info['equity']

        avg_cost = 0.0
        market_price = trade_input.new_price_dict['close']
        pos_size = 0.0
        pos_value = 0.0
        stop_loss = 0.0
        stop_loss_price = 0.0

        # Open Long/Short action just occurred at this bar. So, set profit to 0
        move_stop_loss_datetime = None
        profit_point = 0.0
        profit_percent = 0.0
        profit_money = 0.0
        num_of_scale_in = 0
        num_of_scale_out = 0
        scale_datetime = None
        scale_size = 0.0

        data = [datetime, signal_code, action_code, equity, avg_cost, market_price, pos_size, pos_value \
            , stop_loss, stop_loss_price, move_stop_loss_datetime, profit_point, profit_percent \
            , profit_money, num_of_scale_in, num_of_scale_out, scale_datetime, scale_size]

        return data

    def __build_data_case4(self, trade_input, trade_action, cur_trade):
        datetime = trade_action.datetime
        signal_code = trade_action.signal_code
        action_code = trade_action.action_code
        equity = trade_input.account_info['equity']

        cur_position = cur_trade.get_position()
        avg_cost = self.get_last_row()['avg_cost']
        pos_size = self.get_last_row()['actual_pos_size']
        pos_value = self.get_last_row()['amount']

        entry_price = 0.0
        stop_loss = self.get_last_row()['stop_loss']
        stop_loss_price = self.get_last_row()['stop_loss_price']
        move_stop_loss_datetime = self.get_last_row()['move_stop_loss_datetime']

        # In case of previous action is either hold or scale or open, and current action is open
        # Because must close current trade before open new trade
        if self.__is_open_action(trade_action.action_code):
            entry_price = cur_trade.get_entry_price()
            stop_loss = trade_action.stop_loss
            stop_loss_price = self.__cal_stop_loss_price(trade_action, cur_position, entry_price)
            move_stop_loss_datetime = self.__get_move_stop_loss_datetime(trade_action)

        # *** ตอนนี้ยังเป็น bug เพราะ streaming pro พอปิดสถานะ จะไม่ทราบว่า match ที่ราคาเท่าไร
        # ต้องไปดูเองในหน้า Deal แต่ ณ ตอนนี้ยังไม่ได้ sniff และแกะข้อมูลส่วนนี้ จึงใช้ราคา close แทนไปก่อน
        # ทำให้การคำนวณกำไรอาจคาดเคลื่อนได้เล็กน้อย (slippage)
        market_price = trade_input.new_price_dict['close']

        profit = self.__cal_profit(cur_position, avg_cost, pos_size, market_price)

        profit_point = profit[0]
        profit_percent = profit[1]
        profit_money = profit[2]
        num_of_scale_in = 0
        num_of_scale_out = 0
        scale_datetime = None
        scale_size = 0.0

        data = [datetime, signal_code, action_code, equity, avg_cost, market_price, pos_size, pos_value \
            , stop_loss, stop_loss_price, move_stop_loss_datetime, profit_point, profit_percent \
            , profit_money, num_of_scale_in, num_of_scale_out, scale_datetime, scale_size]

        return data

    def __build_data_case5(self, trade_input, trade_action, cur_trade):
        portfolio_entry = trade_input.cur_trade_portfolio_entry

        datetime = trade_action.datetime
        signal_code = trade_action.signal_code
        action_code = trade_action.action_code
        equity = trade_input.account_info['equity']

        cur_position = portfolio_entry['position']
        market_price = portfolio_entry['market_price']
        avg_cost = portfolio_entry['avg_cost']
        pos_size = portfolio_entry['actual_pos_size']
        pos_value = portfolio_entry['amount']

        entry_price = 0.0
        if self.__is_open_action(self.get_last_row()['action_code']):
            entry_price = avg_cost
        else:
            entry_price = cur_trade.get_entry_price()

        stop_loss = trade_action.stop_loss
        # Stop loss in trade action is point but the following is price
        stop_loss_price = self.__cal_stop_loss_price(trade_action, cur_position, entry_price)

        # Open Long/Short action just occurred at this bar. So, set profit to 0
        move_stop_loss_datetime = None

        profit_point = portfolio_entry['market_price'] - avg_cost
        profit_percent = portfolio_entry['unrealized_profit_percent']
        profit_money = portfolio_entry['unrealized_profit']

        prev_pos_size = self.get_last_row()['pos_size']
        num_of_scale_in = 0
        num_of_scale_out = 0
        scale_datetime = None
        scale_size = 0.0

        # *** กรณีที่ pos size ใหม่กับของก่อาหน้ามีค่าเท่ากัน แสดงว่า pos size ของเดิมมีจำนวนน้อยมาก หรือเพราะเงินไม่พอเปิดเพิ่ม
        if pos_size != prev_pos_size:
            if pos_size > prev_pos_size:
                if self.__is_open_action(self.get_last_row()['action_code']):
                    num_of_scale_in = 1
                else:
                    num_of_scale_in = cur_trade.get_num_of_scale_in() + 1
                scale_size = ((pos_size - prev_pos_size) * 100) / prev_pos_size
            elif pos_size < prev_pos_size:
                if self.__is_open_action(self.get_last_row()['action_code']):
                    num_of_scale_out = 1
                else:
                    num_of_scale_out = cur_trade.get_num_of_scale_out() + 1
                scale_size = ((prev_pos_size - pos_size) * 100) / prev_pos_size

            scale_datetime = trade_action.datetime
            scale_size = round(scale_size, 0)

        data = [datetime, signal_code, action_code, equity, avg_cost, market_price, pos_size, pos_value \
            , stop_loss, stop_loss_price, move_stop_loss_datetime, profit_point, profit_percent \
            , profit_money, num_of_scale_in, num_of_scale_out, scale_datetime, scale_size]

        return data

    def __build_data_case6(self, trade_input, trade_action, cur_trade):
        # Log for closing existing trade
        data_close = self.__build_data_case4(trade_input, trade_action, cur_trade)
        # Log for opening new trade
        data_open = self.__build_data_case1(trade_input, trade_action)

        data = [data_close, data_open]

        return data

    #===================================================================================================================
    # END: Private methods
    #===================================================================================================================

    #===================================================================================================================
    # BEGIN: Add log methods
    #===================================================================================================================
    def add_log(self, trade_input, trade_action, cur_position):
        last_action_code = self.get_last_row()['action_code']
        action_code = trade_action.action_code
        is_case6 = False

        data = None

        # Build data by recognizing last action and new action
        if (self.__is_close_action(last_action_code) or self.__is_wait_action(last_action_code)) \
                and self.__is_open_action(action_code):
            data = self.__build_data_case1(trade_input, trade_action, cur_position)

        elif (self.__is_open_action(last_action_code) or self.__is_hold_action(last_action_code)
                    or self.__is_scale_action(last_action_code)) \
                and self.__is_hold_action(action_code):
            data = self.__build_data_case2(trade_input, trade_action, cur_position)

        elif (self.__is_close_action(last_action_code) == True or self.__is_wait_action(last_action_code) == True) \
                and self.__is_wait_action(action_code) == True:
            data = self.__build_data_case3(trade_input, trade_action, cur_position)

        elif (self.__is_open_action(last_action_code) or self.__is_hold_action(last_action_code)
                    or self.__is_scale_action(last_action_code)) \
                and self.__is_close_action(action_code):
            data = self.__build_data_case4(trade_input, trade_action, cur_position)

        elif (self.__is_open_action(last_action_code) or self.__is_hold_action(last_action_code)
                    or self.__is_scale_action(last_action_code)) \
                and self.__is_scale_action(action_code):
            data = self.__build_data_case5(trade_input, trade_action, cur_position)

        elif (self.__is_open_action(last_action_code) or self.__is_hold_action(last_action_code)
                    or self.__is_scale_action(last_action_code)) \
                and self.__is_open_action(action_code):
            data = self.__build_data_case6(trade_input, trade_action, cur_position)
            is_case6 = True


        # Create new dataframe
        if not(is_case6):
            new_df = pd.DataFrame(data=[data], columns=self.column_list)
        else:
            new_df = pd.DataFrame(data=data, columns=self.column_list)

        self.append_row(new_df)

        return new_df
    #===================================================================================================================
    # END: Add log methods
    #===================================================================================================================
