import datetime
import logging

import deepquant.common.error as err
import deepquant.common.datetime_util as datetime_util
import deepquant.common.state_machine as st_machine
import deepquant.market_fx_mt4.trade_dto as trade_dto

# create logger
logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


# ==============================================================================================================
# BEGIN: Alpha Model
# ==============================================================================================================
class BaseAlphaModel:

    state_machine = None
    # Default trading signal
    default_trade_signal = None

    """
    A constructor of BaseAlphaModel
    """
    def __init__(self):
        self.__robot_context = None
        self.__config = None

        self.state_machine = st_machine.StateMachine()
        self.default_trade_signal = self.state_machine.SIGNAL_NONE

    def set_robot_context(self, robot_context):
        self.__robot_context = robot_context
        self.__config = self.__robot_context.config

    # controller ทำหน้าที่รับผิดชอบการทำงานภายใน BaseAlphaModel ทั้งหมด
    # จะ return ค่ากลับเป็น trading analysis log
    # โดยจะไปเรียกเมธอดอื่นๆ ในคลาส BaseAlphaModel ตามลำดับ
    # Param: trade_input - TradeInput
    def run(self, trade_input):
        try:
            # trade_input.new_price_dict contains date, time, o, h, l, c, v
            # 1) Predict trade signal
            trade_signal_code = self.predict_trade_signal(trade_input)

            # 2) Validate trade signal (filter false break, filter strategy)
            trade_signal_code = self.validate_trade_signal(trade_signal_code, trade_input)
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self.__config['robot_name'], 'INFO' \
                        , "Signal output from AlphaModel is {}".format(trade_signal_code))

            # 3) Build initial trade action
            trade_action = self.build_trade_action(trade_signal_code, trade_input)
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self.__config['robot_name'], 'INFO' \
                        , "Create initial trade action in AlphaModel successful")
        except Exception as e:
            raise err.TradeModelError("BaseAlphaModel.run error: {}".format(e))

        return trade_action

    def predict_trade_signal(self, trade_input):
        trade_signal = self.default_trade_signal

        return trade_signal

    def validate_trade_signal(self, trade_signal, trade_input):
        return trade_signal

    def build_trade_action(self, trade_signal_code, trade_input):
        trade_action = trade_dto.TradeAction()
        trade_action.datetime = trade_input.new_price_dict['date'] + ' ' + trade_input.new_price_dict['time']
        trade_action.symbol = trade_input.symbol
        trade_action.open_price = trade_input.new_price_dict['close']
        trade_action.signal_code = trade_signal_code
        trade_action.slippage = self.__config['default_slippage']

        return trade_action
# ==============================================================================================================
# END: Alpha Model
# ==============================================================================================================


# ==============================================================================================================
# BEGIN: Risk Model
# ==============================================================================================================
class BaseRiskModel:

    state_machine = None
    # Default trading signal
    default_trade_signal = None

    """
    A constructor of BaseRiskModel
    """
    def __init__(self):
        self.__robot_context = None
        self.__config = None

        self.state_machine = st_machine.StateMachine()
        self.default_trade_signal = self.state_machine.SIGNAL_NONE

    def set_robot_context(self, robot_context):
        self.__robot_context = robot_context
        self.__config = self.__robot_context.config

    # controller ทำหน้าที่รับผิดชอบการทำงานภายใน BaseRiskModel ทั้งหมด
    # จะ return ค่ากลับเป็น trading action ที่ปรับรายละเอียดภายในแล้ว
    # โดยจะไปเรียกเมธอดอื่นๆ ในคลาส BaseRiskModel ตามลำดับ
    # Param: trade_input - TradeInput
    # Param: trade_action - TradeAction
    def run(self, trade_input, trade_action):
        try:
            # trade_input.new_price_dict contains date, time, open, high, low, close
            cur_trade_position = trade_input.cur_trade_position
            acc_info_dict = trade_input.acc_info_dict
            open_order_dict = trade_input.open_order_dict
            new_price_dict = trade_input.new_price_dict

            balance = acc_info_dict['balance']  # Get available balance (money)
            free_margin = acc_info_dict['free_margin']  # Get free margin
            leverage = self.__config['trade_leverage']  # Get leverage
            # Get equity outside broker
            equity_outside_broker = self.__config.equity_outside_broker

            # 1) Calculate equity value to be used for calculating position size
            equity = balance + equity_outside_broker

            # 2) Evaluate market risk
            #trade_action = self.eval_market_risk(trade_action, trade_input)

            # 3) Handle open position
            # action_code: 1 = 'OPEN BUY', 2 = 'OPEN SELL'
            if trade_action.action_code == self.state_machine.ACTION_OPEN_BUY \
                    or trade_action.action_code == self.state_machine.ACTION_OPEN_SELL:
                # 3.1) Calculate position size
                trade_action.lot = self.cal_pos_size_of_entry(trade_action, trade_input, equity)

                # 3.2) ตรวจสอบ entry strategy เช่น อาจปรับเพิ่ม/ลด position size หรือยกเลิกเปิดสถานะ
                trade_action = self.eval_entry_strategy(trade_action)

            # 4) Handle hold position
            # action_code: 3 = 'HOLD BUY', 4 = 'HOLD SELL'
            elif trade_action.action_code == self.state_machine.ACTION_HOLD_BUY \
                    or trade_action.action_code == self.state_machine.ACTION_HOLD_SELL:
                # 4.1) ตรวจสอบ exit strategy เช่น ตัดสินใจปิดสถานะก่อนเกิดสัญญาณกลับตัว
                trade_action = self.eval_exit_strategy(trade_action, trade_input)

            # 5) Handle stop loss
            # action_code: 1 = 'OPEN BUY', 2 = 'OPEN SELL', 3 = 'HOLD BUY', 4 = 'HOLD SELL'
            if trade_action.action_code == self.state_machine.ACTION_OPEN_BUY \
                    or trade_action.action_code == self.state_machine.ACTION_OPEN_SELL \
                    or trade_action.action_code == self.state_machine.ACTION_HOLD_BUY \
                    or trade_action.action_code == self.state_machine.ACTION_HOLD_SELL:
                # 5.1) กำหนดตำแหน่งวาง stop loss
                trade_action = self.define_stoploss_position(trade_action, trade_input)

                # 5.2) กำหนดระยะทำกำไร หรือ target profit
                trade_action = self.define_target_profit(trade_action, trade_input)

        except Exception as e:
            raise err.TradeModelError("BaseRiskModel.run error: {}".format(e))

        return trade_action

    # Calculate risk โดยประเมินจากจำนวนเปอร์เซ็นต์การขาดทุนสูงสุดที่รับได้จากเงินทุน (equity) ที่มี
    def cal_risk_to_take(self, trade_action, trade_input, equity):
        risk = 0
        return risk

    def cal_pos_size_of_entry(self, trade_action, trade_input, equity):
        # คำนวณความเสี่ยงของเทรดนี้ ซึ่งหมายถึง จ.น. เปอร์เซ็นต์ที่ยอมขาดทุนได้ในเทรดนี้
        risk = self.cal_risk_to_take(trade_action, trade_input, equity)

        acc_info_dict = trade_input.acc_info_dict

        point_value = self.__config['point_value']
        # default stop loss
        default_sl = self.__config['default_sl']
        # จ.น.ทศนิยมสูงสุดที่โบรกฯ/ตลาดอนุญาตให้ใช้กับ pos. size ได้
        pos_size_decimal_num = self.__config['pos_size_decimal_num']

        # คำนวณขนาดสัญญา
        pos_size = (equity * risk) / (default_sl * point_value)  # คำนวณขนาด Lot

        return round(pos_size, pos_size_decimal_num)

    def eval_entry_strategy(self, trade_action, trade_input, max_decimal):
        return trade_action

    def eval_exit_strategy(self, trade_action, trade_input, max_decimal):
        return trade_action

    def eval_scale_out(self, trade_action, trade_input, max_decimal):
        return trade_action

    def eval_scale_in(self, trade_action, trade_input, max_decimal):
        return trade_action

    def define_stoploss_position(self, trade_action, trade_input, max_decimal):
        return trade_action

    def define_target_profit(self, trade_action, trade_input, max_decimal):
        return trade_action
# ==============================================================================================================
# END: Risk Model
# ==============================================================================================================


# ==============================================================================================================
# BEGIN: Transaction Cost Model
# ==============================================================================================================
class BaseTransCostModel :

    state_machine = None

    def __init__(self, comm_table=None):
        self.__comm_table = comm_table
        self.__robot_context = None
        self.__config = None

        self.state_machine = st_machine.StateMachine()

    def set_robot_context(self, robot_context):
        self.__robot_context = robot_context
        self.__config = self.__robot_context.config

    # controller ทำหน้าที่รับผิดชอบการทำงานภายใน BaseTransCostModel ทั้งหมด
    # จะ return ค่ากลับเป็น trading action ที่ปรับรายละเอียดภายในแล้ว
    # โดยจะไปเรียกเมธอดอื่นๆ ในคลาส BaseTransCostModel ตามลำดับ
    # Param: trade_input - TradeInput
    # Param: trade_action - TradeAction
    def run(self, trade_input, trade_action):
        try:
            # 1) Set price for open position
            # action_code: 1 = 'OPEN BUY', 2 = 'OPEN SELL'
            if trade_action.action_code == self.state_machine.ACTION_OPEN_BUY:
                trade_action.open_price = self.__config['ask_price']

            elif trade_action.action_code == self.state_machine.ACTION_OPEN_SELL:
                trade_action.open_price = self.__config['bid_price']

            # 2) Set slippage
            trade_action.slippage = self.cal_slippage()

        except Exception as e:
            raise err.TradeModelError("BaseTransCostModel.run error: {}".format(e))

        return trade_action

    # Return อัตราค่า commission โดยดูจาก position size
    # pos_size - position size
    def _get_comm_rate(self, pos_size):
        comm_rate = self.__comm_table.get_comm_rate(pos_size)
        # NOTE: template นี้ใช้ค่าคอมมิสชั่นอัตราปกติ หากคุณได้อัตราค่าคอมฯ อื่น ให้สร้างคลาส TransCostModel
        # แล้ว inherit คลาส BaseTransCostModel นี้ แล้ว override เมธอด __get_comm_rate() นี้ใหม่
        return comm_rate

    def get_spread(self, trade_action):
        spread = self.__config['default_spread']
        return spread

    # คำนวณค่า commission
    # pos_size - position size
    def cal_commission(self, pos_size):
        comm_rate = self._get_comm_rate(pos_size)
        comm = comm_rate * pos_size

        return comm

    # คำนวณค่า slippage
    def cal_slippage(self):
        slippage = self.__config['default_slippage']
        return slippage

    def cal_swap_cost(self):
        swap_cost = 0.0
        # ไม่ได้เขียนตัวอย่างในส่วนนี้
        return swap_cost

    # ตรวจสอบ transaction cost
    def eval_transcost(self):
        result = False
        # ไม่ได้เขียนตัวอย่างในส่วนนี้
        return result

# ==============================================================================================================
# END: Transaction Cost Model
# ==============================================================================================================


# ==============================================================================================================
# BEGIN: Portfolio Construction Model
# ==============================================================================================================
class BasePortfolioModel:

    state_machine = None

    """
    A constructor of BasePortfolioModel
    """
    def __init__(self):
        self.__robot_context = None
        self.__config = None
        self.state_machine = st_machine.StateMachine()

    def set_robot_context(self, robot_context):
        self.__robot_context = robot_context
        self.__config = self.__robot_context.config

    # controller ทำหน้าที่รับผิดชอบการทำงานภายใน BasePortfolioModel ทั้งหมด
    # จะ return ค่ากลับเป็น trading action ที่ปรับรายละเอียดภายในแล้ว
    # โดยจะไปเรียกเมธอดอื่นๆ ในคลาส BasePortfolioModel ตามลำดับ
    # Param: trade_input - TradeInput
    # Param: trade_action - TradeAction
    def run(self, trade_input, trade_action):
        try:
            trade_action.lot = self.validate_pos_size_limit(trade_input, trade_action)
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self.__config['robot_name'], 'INFO' \
                        , "PortfolioModel: validate pos. size successful. Pos. size is " + str(trade_action.lot))
        except Exception as e:
            raise err.TradeModelError("BasePortfolioModel.run error: {}".format(e))

        return trade_action

    def validate_pos_size_limit(self, trade_input, trade_action):
        pos_size = trade_action.volume
        limit = self.__config['limit_pos_size']

        if pos_size > limit :
            pos_size = limit

        return pos_size
# ==============================================================================================================
# END: Portolio Construction Model
# ==============================================================================================================
