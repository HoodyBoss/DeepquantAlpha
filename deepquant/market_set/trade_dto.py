import deepquant.common.json_util as json_util


# This module implements DTO (Data Transfer Object) Design Pattern for encapsulate data transfering between modules
class TradeInput:
    symbol = None

    # The following 3 variables will be set by execution_model
    portfolio = None
    orders = None
    account_info = None
    cur_trade_portfolio_entry = None

    # The following variable will be set by portfolio_model
    cur_trade_position = None


    def __init__(self, symbol, new_price_dict=None, price_df=None, indi_df=None, feature_df=None, signal_dict=None, transaction_dict=None):
        """
        A constructor of TradeInput
        :param symbol: a symbol to trade (sent from frontend robot)
        :param new_price_dict: A dictionary type for latest price
                               (date, time, open, high, low, close, volume, ...)
        :param price_df: price dataframe contains multiple rows
        :param indi_df:  indicator dataframe contains multiple rows
        :param feature_df: feature dataframe contain ONLY ONE latest row
        :param signal_dict: A dictionary type for signal details
                เราอาจเขียนโค้ดส่วน Alpha Model เช่น rule based system ต่างๆ ที่ frontend robot (Amibroker/MT4/5) เลยก็ได้
                แล้วจึงส่ง output มาที่ฝั่ง python robot นี้ เพื่อประมวลผลในส่วนอื่นต่อไป
                เนื่องจาก frontend robot จะไม่ได้เชื่อมต่อกับตลาด จึงไม่สามารถดึงข้อมูลพอร์ตโฟลิโอ เช่น เงิน, สถานะ ฯลฯ ได้
                จึงต้องส่ง output จากการประมวลผลเบื้องต้นนั้นมาทำต่อที่ฝั่งนี้
                               (signal_code, stop_loss, entry_pos_size_percent, scale_size)
                signal_code - ให้อ้างอิง signal code ใน deepquant.common.state_machine
                stop_loss - ต้องเป็นจำนวนจุด ไม่ใช่ราคาหรือดัชนี เช่น ส่งค่า 10.0 มา
                entry_pos_size_percent - ส่งค่ามาเป็นเปอร์เซ็นต์ เช่น 1.0 = 100 %, 0.5 = 50 %
                scale_size - ส่งค่ามาเป็นเปอร์เซ็นต์ เช่น 0.5 = 50 %, 0.3 = 30 %
        :param transaction_dict: A dictionary type for transaction record
                               (trans_id, created_dt, trans_status, trans_status_dt)
                               Note: created_dt - date time (yyyy-MM-dd HH:mm:ss) in string
                                    trans_status_dt - timestamp of transaction status (yyyy-MM-dd HH:mm:ss) in string
        """
        self.symbol = symbol
        self.new_price_dict = new_price_dict
        self.price_df = price_df
        self.indi_df = indi_df
        self.feature_df = feature_df
        self.signal_dict = signal_dict
        self.transaction_dict = transaction_dict

    def clone(self):
        clone_symbol = self.symbol
        clone_new_price_dict = self.new_price_dict
        clone_price_df = self.price_df
        clone_indi_df = self.indi_df
        clone_feature_df = self.feature_df
        clone_signal_dict = self.signal_dict
        clone_transaction_dict = self.transaction_dict

        clone_trade_input = TradeInput(clone_symbol, clone_new_price_dict, clone_price_df\
                                       , clone_indi_df, clone_feature_df, clone_signal_dict\
                                       , clone_transaction_dict)
        return clone_trade_input


# This module implements DTO (Data Transfer Object) Design Pattern for encapsulate data transfering between modules
class TradeOutput:

    trade_account_id    = ''
    trade_action        = None
    http_success_code   = 200
    http_error_code     = 500

    def __init__(self):
        self.response_code      = self.http_error_code
        self.response_message   = ''

    def to_json(self):
        trade_output_dict = \
            {
                'response_code'    : self.response_code,
                'response_message' : self.response_message
            }

        if self.trade_account_id is not '':
            trade_output_dict['trade_account_id'] = self.trade_account_id

        # Convert to JSON
        trade_output_json = json_util.encode(trade_output_dict)
        return trade_output_json

    def get_response_message(self):
        return self.to_json()


class TfexTradeAction:

    def __init__(self):
        self.signal_code        = 0
        self.action_code        = 0
        self.datetime           = ''
        self.symbol             = ''
        self.volume             = 0
        self.action_price       = 0.0
        self.slippage           = 0.0
        self.stop_loss          = 0.0   # NOTE: ระบุเป็นจำนวนจุด ไม่ใช่ราคาหรือดัชนี

    def to_json(self):
        # Convert to dictionary
        trade_action_dict = \
                {
                    'signal_code'       : self.signal_code,
                    'action_code'       : self.action_code,
                    'datetime'          : self.datetime,
                    'symbol'            : self.symbol,
                    'volume'            : self.volume,
                    'slippage'          : self.slippage,
                    'action_price'      : self.action_price,
                    'stop_loss'         : self.stop_loss
                }

        # Convert to JSON
        trade_action_json = json_util.encode(trade_action_dict)

        return trade_action_json

    def to_dict(self):
        # Convert to dictionary
        trade_action_dict = \
                {
                    'signal_code': self.signal_code,
                    'action_code'       : self.action_code,
                    'datetime'          : self.datetime,
                    'symbol'            : self.symbol,
                    'volume'            : self.volume,
                    'slippage'          : self.slippage,
                    'action_price'      : self.action_price,
                    'stop_loss'         : self.stop_loss
                }

        return trade_action_dict

    def clone(self):
        clone_signal_code = self.signal_code
        clone_action_code = self.action_code
        clone_datetime = self.datetime
        clone_symbol = self.symbol
        clone_volume = self.volume
        clone_slippage = self.slippage
        clone_action_price = self.action_price
        clone_stop_loss = self.stop_loss

        clone_trade_action = TfexTradeAction()
        clone_trade_action.signal_code = clone_signal_code
        clone_trade_action.action_code = clone_action_code
        clone_trade_action.datetime = clone_datetime
        clone_trade_action.symbol = clone_symbol
        clone_trade_action.volume = clone_volume
        clone_trade_action.slippage = clone_slippage
        clone_trade_action.action_price = clone_action_price
        clone_trade_action.stop_loss = clone_stop_loss

        return clone_trade_action



"""
# =======================================================================================================================
# TEST TradeAction
# =======================================================================================================================
xxxx


#=======================================================================================================================
# TEST TradeInput
#=======================================================================================================================

import pandas
import datetime
import json
symbol = 'SET50'
columns=['symbol','date','time','open','high','low','close','vol'
         ,'daily_vol','daily_rsi','daily_stochk','daily_stochd'
         ,'daily_macd','daily_macdsignal']
new_price_df = pandas.DataFrame([['SET50', '20180221', '121400'
                    , 1111.1, 2222.2, 3333.3, 4444.4
                    , 55555, 66666
                    , 11.1, 12.2, 13.3
                    , 14.4, 15.5]], columns=columns)
new_prices_json = new_price_df.loc[0,].to_json()

datetime_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
trans_id = int(datetime.datetime.now().strftime('%s'))

trans_df = pandas.DataFrame([[trans_id, datetime_str, 'A', datetime_str]],
                                    columns=['trans_id', 'created_dt', 'trans_status', 'trans_status_dt'])
trans_json = trans_df.loc[0,].to_json()

trade_input_json = '{\"new_prices\":' + new_prices_json\
                    + ', \"transaction\":' + trans_json + '}'
print(trade_input_json)

print('.............................')

trade_input_dict = json.JSONDecoder().decode(trade_input_json)
print(trade_input_dict)

"""

