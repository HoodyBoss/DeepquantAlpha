from flask import current_app
from flask_redis import FlaskRedis
import pandas as pd

import deepquant.common.state_machine as state_machine

try:
   from six.moves import cPickle as pickle
except:
   import pickle


# This is a wrapper class for trading list, encapsulating session using Redis
class TradeListContext :

    column_list = None

    # A constructor of TradeListContext
    # Param: name - a robot name
    def __init__(self, robot_name, robot_config):
        self.cache_key = robot_name + "_trade_list"
        self.db = FlaskRedis(current_app)
        self.config = robot_config
        self.df = self.get()
        self.__st_machine = state_machine.StateMachine()
        self.__state = state_machine.State()
        self.column_list = ['position', 'entry_datetime', 'entry_price', 'entry_pos_size', 'exit_datetime', 'exit_price' \
            , 'equity', 'avg_cost', 'pos_size', 'pos_value', 'stop_loss', 'stop_loss_price', 'profit_point' \
            , 'profit_percent', 'profit_money', 'highest_high', 'highest_close', 'lowest_low', 'lowest_close' \
            , 'max_profit', 'max_dd_percent', 'max_dd_period', 'hold_bars', 'num_of_scale_in', 'num_of_scale_out' \
            , 'loss_type', 'win', 'entry_type', 'exit_type', 'mae', 'mfe']

    #===================================================================================================================
    # BEGIN: Common accessing methods
    #===================================================================================================================
    def get_last_row(self):
        """
        Return last row  of trade details log containing all columns

        Returns:
            Dataframe
        """

        return self.df[-1:]

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
        self.df = self.df.drop(self.df.index[[0]])
        self.set(self.df)

    def append_row(self, new_row_df):
        """
        Append new row to existing data
        """
        df = self.df.append(new_row_df)
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

    def set_field(self, field_name, new_value):
        self.get_last_row()[field_name] = new_value # using pass by reference
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

    def __recognize_position(self, portfolio_entry):
        position = 0
        cur_position = portfolio_entry['position']
        if self.__st_machine.decode_state(cur_position) == self.__state.STATE_BUY:
            position = self.__state.STATE_BUY
        elif self.__st_machine.decode_state(cur_position) == self.__state.STATE_SELL:
            position = self.__state.STATE_SELL

        return position

    def __cal_mae(self, entry_price, price_dict, portfolio_entry):
        result = 0.0
        if self.__recognize_position(portfolio_entry) == self.__state.STATE_BUY \
                and price_dict['close'] <= entry_price:
            result = price_dict['low'] - entry_price
        elif self.__recognize_position(portfolio_entry) == self.__state.STATE_SELL \
                and price_dict['close'] >= entry_price:
            result = entry_price - price_dict['high']

        return result

    def __cal_mfe(self, exit_price, max_profit):
        result = exit_price - max_profit
        return result

    def __cal_max_dd(self, trade_detail_df):
        percent = 0.0
        period = 0

        cur_datetime64 = 0
        datetime_arr = pd.to_datetime(trade_detail_df['datetime']).as_matrix()
        """
        When previous bar's trade action was either open/hold/scale and current action is open
        the current bar will have 2 trade details log rows, first for close previous trade, second for open new trade.
        Usually 1 trade details log per bar.
        """
        if len(datetime_arr) == 2:
            cur_datetime64 = pd.to_datetime(trade_detail_df['datetime']).as_matrix()[1]
        elif len(datetime_arr) == 1:
            cur_datetime64 = pd.to_datetime(trade_detail_df['datetime']).as_matrix()[0]

        trade_list_df = self.get()
        equity_arr = trade_list_df['equity'].as_matrix()
        max_equity = equity_arr.max()
        max_equity_index = equity_arr.argmax()
        max_equity_datetime = pd.to_datetime(trade_list_df['exit_datetime']).as_matrix()[max_equity_index]
        min_equity = equity_arr.min()

        cur_equity = trade_detail_df['equity'][0]
        if cur_equity < max_equity:
            range = max_equity - min_equity
            drawdown = max_equity - cur_equity
            percent = (drawdown * 100) / range
            period = int((cur_datetime64 - max_equity_datetime) / (24 * 60 * 60 * 1000000000))

        result = [percent, period]
        return result

    def __cal_continual(self, price_dict, trade_detail_df):
        last_hold_bars = self.get_field('hold_bars')

        if last_hold_bars == 0:
            highest_high = price_dict['high']
            highest_close = price_dict['close']
            lowest_low = price_dict['low']
            lowest_close = price_dict['close']
            max_profit = trade_detail_df['profit_point'][0]
            hold_bars = 1
        elif last_hold_bars > 0:
            highest_high = max(price_dict['high'], self.get_field('highest_high'))
            highest_close = max(price_dict['close'], self.get_field('highest_close'))
            lowest_low = min(price_dict['low'], self.get_field('lowest_low'))
            lowest_close = min(price_dict['close'], self.get_field('lowest_close'))
            max_profit = max(trade_detail_df['profit_point'][0], self.get_field('profit_point'))
            hold_bars = last_hold_bars + 1

        data = [highest_high, highest_close, lowest_low, lowest_close, max_profit, hold_bars]
        return data

    def __build_data_case1(self, trade_detail_df, trade_input):
        portfolio_entry = trade_input.cur_trade_portfolio_entry
        position = self.__recognize_position(portfolio_entry) # long/buy=1, short/sell=2

        entry_datetime = trade_detail_df['datetime'][0]
        entry_price = trade_detail_df['avg_cost'][0]
        entry_pos_size = trade_detail_df['pos_size'][0]
        exit_datetime = None
        exit_price = 0.0
        equity = trade_detail_df['equity'][0]
        avg_cost = trade_detail_df['avg_cost'][0]
        pos_size = trade_detail_df['pos_size'][0]
        pos_value = trade_detail_df['pos_value'][0]
        stop_loss = trade_detail_df['stop_loss'][0]
        stop_loss_price = trade_detail_df['stop_loss_price'][0]
        profit_point = trade_detail_df['profit_point'][0]
        profit_percent = trade_detail_df['profit_percent'][0]
        profit_money = trade_detail_df['profit_money'][0]
        highest_high = 0.0
        highest_close = 0.0
        lowest_low = 0.0
        lowest_close = 0.0
        max_profit = 0.0

        max_dd = self.__cal_max_dd(trade_detail_df)
        max_dd_percent = max_dd[0]
        max_dd_period = max_dd[1]

        hold_bars = 0
        num_of_scale_in = trade_detail_df['num_of_scale_in'][0]
        num_of_scale_out = trade_detail_df['num_of_scale_out'][0]
        loss_type = 0 # long/buy=1, short/sell=2, not loss=0
        win = 0 # win=1, loss=2, hold/idle=0

        # *** ยังไม่ได้อิมพลีเม้นต์ Entry type และ exit type เวอร์ชั่นปัจจุบันยังไม่รองรับการเซ็ต
        # Entry type: (example: 1=normal, 2=long at high zone, 3=long at low zone, 4=short at high zone, 5=short at low zone)
        entry_type = 0
        # Exit type: (1=normal, 2=ruin, 3=max loss, 4=trailing stop, 5=profit)
        exit_type = 0

        mae = self.__cal_mae(entry_price, trade_input.new_price_dict)
        mfe = 0.0

        data = [position, entry_datetime, entry_price, entry_pos_size, exit_datetime, exit_price \
            , equity, avg_cost, pos_size, pos_value, stop_loss, stop_loss_price, profit_point \
            , profit_percent, profit_money, highest_high, highest_close, lowest_low, lowest_close \
            , max_profit, max_dd_percent, max_dd_period, hold_bars, num_of_scale_in, num_of_scale_out \
            , loss_type, win, entry_type, exit_type, mae, mfe]

        return data

    def __build_data_case2(self, trade_detail_df, trade_input):
        portfolio_entry = trade_input.cur_trade_portfolio_entry

        self.set_field('equity', trade_detail_df['equity'][0])
        self.set_field('avg_cost', trade_detail_df['avg_cost'][0])
        self.set_field('pos_size', trade_detail_df['pos_size'][0])
        self.set_field('pos_value', trade_detail_df['pos_value'][0])
        self.set_field('stop_loss', trade_detail_df['stop_loss'][0])
        self.set_field('stop_loss_price', trade_detail_df['stop_loss_price'][0])
        self.set_field('profit_point', trade_detail_df['profit_point'][0])
        self.set_field('profit_percent', trade_detail_df['profit_percent'][0])
        self.set_field('profit_money', trade_detail_df['profit_money'][0])

        continual_val = self.__cal_continual(trade_input.new_price_dict, trade_detail_df)

        self.set_field('highest_high', continual_val[0])
        self.set_field('highest_close', continual_val[1])
        self.set_field('lowest_low', continual_val[2])
        self.set_field('lowest_close', continual_val[3])
        self.set_field('max_profit', continual_val[4])
        self.set_field('hold_bars', continual_val[5])

        max_dd = self.__cal_max_dd(trade_detail_df)
        self.set_field('max_dd_percent', max_dd[0])
        self.set_field('max_dd_period', max_dd[1])

        mae = self.__cal_mae(self.get_field('entry_price'), trade_input.new_price_dict, portfolio_entry)
        self.set_field('mae', mae)

        return self.get_last_row()

    def __build_data_case3(self, trade_detail_df, trade_input):
        # Update many fields same as in __build_data_case2 (hold action)
        self.__build_data_case2(trade_detail_df, trade_input)

        self.set_field('num_of_scale_in', trade_detail_df['num_of_scale_in'][0])
        self.set_field('num_of_scale_out', trade_detail_df['num_of_scale_out'][0])

        return self.get_last_row()

    def __build_data_case4(self, trade_detail_df, trade_input):
        # Update many fields same as in __build_data_case2 (hold action)
        self.__build_data_case2(trade_detail_df, trade_input)

        self.set_field('exit_datetime', trade_detail_df['datetime'][0])
        self.set_field('exit_price', trade_detail_df['market_price'][0])

        # 1=long/buy, 2=short/sell
        loss_type = 1
        if self.get_field('position') == self.__state.STATE_SELL:
            loss_type = 2

        self.set_field('loss_type', loss_type) # not implemented yet

        # 1=win, 2=loss
        win = 2
        if trade_detail_df['profit_point'] > 0:
            win = 1

        self.set_field('win', win)
        self.set_field('exit_type', 0) # not implemented yet
        self.set_field('mfe', self.__cal_mfe(trade_input.new_price_dict['close'], self.get_field('max_profit')))

        return self.get_last_row()

    def __build_data_case5(self, trade_detail_df, trade_input):
        # Handle close existing trade
        self.__build_data_case4(trade_detail_df, trade_input)
        # Handle open new trade
        data = self.__build_data_case1(trade_detail_df, trade_input)

        return data

    # ===================================================================================================================
    # END: Private methods
    # ===================================================================================================================

    #===================================================================================================================
    # BEGIN: Add log methods
    #===================================================================================================================
    def add_log(self, trade_detail_df, trade_input, trade_action, cur_position):
        last_action_code = self.get_last_row()['action_code']
        action_code = trade_action.action_code
        is_case1 = False
        is_case5 = False

        data = None

        # Build data by recognizing last action and new action
        if (self.__is_close_action(last_action_code) or self.__is_wait_action(last_action_code)) \
                and self.__is_open_action(action_code):
            data = self.__build_data_case1(trade_input, trade_action, cur_position)
            is_case1 = True

        elif (self.__is_open_action(last_action_code) or self.__is_hold_action(last_action_code)
                    or self.__is_scale_action(last_action_code)) \
                and self.__is_hold_action(action_code):
            data = self.__build_data_case2(trade_input, trade_action, cur_position)

        elif (self.__is_open_action(last_action_code) or self.__is_hold_action(last_action_code)
                    or self.__is_scale_action(last_action_code)) \
                and self.__is_scale_action(action_code):
            data = self.__build_data_case3(trade_input, trade_action, cur_position)

        elif (self.__is_open_action(last_action_code) or self.__is_hold_action(last_action_code)
                    or self.__is_scale_action(last_action_code)) \
                and self.__is_close_action(action_code):
            data = self.__build_data_case4(trade_input, trade_action, cur_position)

        elif (self.__is_open_action(last_action_code) or self.__is_hold_action(last_action_code)
                    or self.__is_scale_action(last_action_code)) \
                and self.__is_open_action(action_code):
            data = self.__build_data_case5(trade_input, trade_action, cur_position)
            is_case5 = True


        # Update existing dataframe
        self.set(self.df)
        # Create new dataframe
        new_df = None
        if is_case1 or is_case5:
            new_df = pd.DataFrame(data=[data], columns=self.column_list)
            self.append_row(new_df)

        return new_df
    # ===================================================================================================================
    # END: Add log methods
    # ===================================================================================================================
