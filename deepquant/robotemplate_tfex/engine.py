import datetime
import logging

import deepquant.common.error as err
import deepquant.common.json_util as json_util
import deepquant.common.datetime_util as datetime_util
import deepquant.common.state_machine as st_machine
import deepquant.common.mq_client as mq_client
import deepquant.market_set.trade_dto as trade_dto

# create logger
logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
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

            # 3) Build initial trade action
            trade_action = self.build_trade_action(trade_signal_code, trade_input)
        except Exception as e:
            raise err.TradeModelError("BaseAlphaModel.run error: {}".format(e))

        return trade_action

    def predict_trade_signal(self, trade_input):
        trade_signal = self.default_trade_signal

        return trade_signal

    def validate_trade_signal(self, trade_signal, trade_input):
        return trade_signal

    def build_trade_action(self, trade_signal_code, trade_input):
        trade_action = trade_dto.TfexTradeAction()
        trade_action.datetime = trade_input.new_price_dict['date'] + ' ' + trade_input.new_price_dict['time']
        trade_action.symbol = trade_input.symbol
        trade_action.action_price = trade_input.new_price_dict['close']
        trade_action.signal_code = trade_signal_code

        #If you want to use limit price, use the following line
        #trade_action.action_price = trade_input.new_price_dict['close']

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
            # trade_input.new_price_dict contains date, time, o, h, l, c
            cur_trade_position = trade_input.cur_trade_portfolio_entry['position']
            acc_info = trade_input.account_info
            new_price_dict = trade_input.new_price_dict

            avg_cost = 0.0;
            # ถ้าเปิดสถานะไม้แรก avg_cost จะมีค่าเป็น 0 เพราะจังหวะนี้ยังไม่ได้ส่งออร์เดอร์
            if trade_input.cur_trade_portfolio_entry['avg_cost'] > 0:
                avg_cost = trade_input.cur_trade_portfolio_entry['avg_cost']

            # Get available equity in portfolio (Streaming Pro)
            equity = acc_info['equity']
            # Get equity outside broker
            equity_outside_broker = self.__config.equity_outside_broker

            # 1) Calculate equity value to be used for calculating position size
            equity = equity + equity_outside_broker

            # 2) Evaluate market risk
            #trade_action = self.eval_market_risk(trade_action, cur_trade_portfolio_entry, new_price_dict)

            # 3) Handle open position
            # action_code: 1 = 'OPEN BUY', 2 = 'OPEN SELL'
            if trade_action.action_code == self.state_machine.ACTION_OPEN_BUY\
                    or trade_action.action_code == self.state_machine.ACTION_OPEN_SELL:
                # 3.1) Calculate position size
                trade_action.volume = self.cal_pos_size_of_entry(trade_input, equity)

                # 3.2) ตรวจสอบ entry strategy เช่น อาจปรับเพิ่ม/ลด position size หรือยกเลิกเปิดสถานะ
                trade_action = self.eval_entry_strategy(trade_action, trade_input)

            # 4) Handle hold position
            # action_code: 3 = 'HOLD BUY', 4 = 'HOLD SELL'
            if trade_action.action_code == self.state_machine.ACTION_HOLD_BUY\
                    or trade_action.action_code == self.state_machine.ACTION_HOLD_SELL:
                # 4.1) ตรวจสอบ exit strategy เช่น ตัดสินใจปิดสถานะก่อนเกิดสัญญาณกลับตัว
                trade_action = self.eval_exit_strategy(trade_action, trade_input)

            # 5) Handle stop loss
            # action_code: 1 = 'OPEN BUY', 2 = 'OPEN SELL', 3 = 'HOLD BUY', 4 = 'HOLD SELL'
            if trade_action.action_code == self.state_machine.ACTION_OPEN_BUY\
                    or trade_action.action_code == self.state_machine.ACTION_OPEN_SELL\
                    or trade_action.action_code == self.state_machine.ACTION_HOLD_BUY\
                    or trade_action.action_code == self.state_machine.ACTION_HOLD_SELL:

                # 5.1) กำหนดตำแหน่งวาง stop loss
                trade_action = self.define_stoploss_position(trade_action, trade_input)

                # 5.2) กำหนดระยะทำกำไร หรือ target profit
                # NOTE: ใน TFEX ไม่มีให้กำหนด target profit
        except Exception as e:
            raise err.TradeModelError("BaseRiskModel.run error: {}".format(e))

        return trade_action

    # Calculate risk โดยประเมินจากจำนวนเปอร์เซ็นต์การขาดทุนสูงสุดที่รับได้จากเงินทุน (equity) ที่มี
    def cal_risk_to_take(self, equity):
        risk = 0
        return risk

    def cal_pos_size_of_entry(self, trade_input, equity):
        # คำนวณความเสี่ยงของเทรดนี้ ซึ่งหมายถึง จ.น. เปอร์เซ็นต์ที่ยอมขาดทุนได้ในเทรดนี้
        risk = self.cal_risk_to_take(equity)

        acc_info = trade_input.account_info

        point_value = self.__config['point_value']
        # Default stop loss
        default_sl = self.__config['default_sl']
        # จ.น.ทศนิยมสูงสุดที่โบรกฯ/ตลาดอนุญาตให้ใช้กับ pos. size ได้
        pos_size_decimal_num = self.__config['pos_size_decimal_num']

        # คำนวณขนาดสัญญา
        pos_size = (equity * risk) / (default_sl * point_value)

        return round(pos_size, pos_size_decimal_num)

    def eval_entry_strategy(self, trade_action, trade_input):
        return trade_action

    def eval_exit_strategy(self, trade_action, trade_input):
        return trade_action

    def eval_scale_out(self, trade_action, trade_input):
        return trade_action

    def eval_scale_in(self, trade_action, trade_input):
        return trade_action

    def define_stoploss_position(self, trade_action, trade_input):
        return trade_action

    def define_target_profit(self, trade_action, cur_trade_position, open_order, trade_input):
        return trade_action
# ==============================================================================================================
# END: Risk Model
# ==============================================================================================================


# ==============================================================================================================
# BEGIN: Transaction Cost Model
# ==============================================================================================================
class BaseTransCostModel :

    state_machine = None

    """
    A constructor of BaseTransCostModel
    index of this dataframe is column 'from' and 'to'
    sample comm_table of TFEX (S50xxx) ->
    |   from    |   to  |   comm rate (Baht)/size   |
    |===========|=======|===========================|
    |   1       |   25  |   85                      |
    |   26      |   100 |   63                      |
    """
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
            if trade_action.action_code == self.state_machine.ACTION_OPEN_BUY\
                    or trade_action.action_code == self.state_machine.ACTION_OPEN_SELL:

                # Set open_price to 0.0 -> หมายถึงต้องการเปิดสถานะด้วยราคา market
                # แต่ถ้าไม่เป็น 0.0 หมายถึงต้องการเปิดสถานะด้วยราคานี้
                trade_action.open_price = 0.0

            # 2) Set slippage
            #trade_action.slippage = self.cal_slippage()
            # ตัวอย่างโค้ดเป็นการใช้ราคา market
        except Exception as e:
            raise err.TradeModelError("BaseTransCostModel.run error: {}".format(e))

        return trade_action

    # Return อัตราค่า commission โดยดูจาก position size
    # pos_size - position size
    def _get_comm_rate(self, pos_size):
        comm_rate = self.__comm_table.get_comm_rate(pos_size)
        # NOTE: template นี้ใช้ค่าคอมมิสชั่นอัตราปกติ หากคุณได้อัตราค่าคอมฯ อื่น ให้สร้างคลาส TransCostModel ใน /tradingrobot/xxx/engine
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

    def set_cur_trade_position(self, trade_input):
        cur_trade_portfolio_str = trade_input.cur_trade_portfolio_entry['position']
        trade_input.cur_trade_position = self.state_machine.decode_state(cur_trade_portfolio_str)
        return trade_input

    # controller ทำหน้าที่รับผิดชอบการทำงานภายใน BasePortfolioModel ทั้งหมด
    # จะ return ค่ากลับเป็น trading action ที่ปรับรายละเอียดภายในแล้ว
    # โดยจะไปเรียกเมธอดอื่นๆ ในคลาส BasePortfolioModel ตามลำดับ
    # Param: trade_input - TradeInput
    # Param: trade_action - TradeAction
    def run(self, trade_input, trade_action):
        try:
            trade_action.volume = self.validate_pos_size_limit(trade_input, trade_action)
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self.__config['robot_name'], 'INFO' \
                        , "PortfolioModel: validate pos. size successful. Pos. size is " + str(trade_action.volume))
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


# ==============================================================================================================
# BEGIN: Execution Model
# ==============================================================================================================
class BaseExecutionModel:

    state_machine = None

    """
    A constructor of BaseExecutionModel
    """
    def __init__(self):
        self.__robot_context = None
        self.__config = None
        self.state_machine = st_machine.StateMachine()

    def set_robot_context(self, robot_context):
        self.__robot_context = robot_context
        self.__config = self.__robot_context.config

    # Login to market (Settrade) and load portfolio's entires, order list, account info,
    # 1) portfolio is a dictionary.
    # ในเวอร์ชั่นปัจจุบัน portfolio มีแค่อิลิเม้นต์เดียว: key คือ 'entries', มี value เป็น list of dictionary
    # แต่ละ portfolio entry dictionary มี key ดังนี้:
    # symbol คือ ชื่อซีรีส์ที่เทรด เช่น S50Z18
    # position คือ สถานะ เช่น Long, Short
    # start_pos_size คือ จ.น.สัญญาที่เปิดไม้แรก
    # available_pos_size คือ จ.น.สัญญาที่เหลืออยู่ของสถานะที่ยังไม่ปิด ที่ยังไม่ได้ส่งออร์เดอร์ไปทำอะไร เช่น ไม่ได้ส่ง close order, stop order
    # actual_pos_size คือ จ.น.สัญญาที่มีอยู่จริงๆ ของสถานะที่ยังไม่ปิด
    # avg_cost คือ ต้นทุนเฉลี่ย *ตลาดบ้านเราไม่จำให้นะครับ ว่าเราเปิดแต่ละไม้ที่ราคาเท่าไหร่ ตลาดจะเอาราคาทุกไม้มารวมกันแล้วคำนวณราคาเฉลี่ย
    # market_price คือ ราคาหรือดัชนี ณ ขณะนั้น
    # amount คือ ขนาดสถานะเทียบเท่าเป็น จ.น.เงิน เมื่อคำนวณร่วมกับ จ.น.สัญญา, avg_cost และ leverage แล้ว
    # market_value คือ เหมือน amount แต่ไม่ได้คำนวณจาก avg_cost แต่ใช้ market_price มาคำนวณ
    # unrealized_profit คือ กำไร/ขาดทุนของสถานะที่ยังไม่ปิด หน่วยเป็นบาท
    # unrealized_profit_percent คือ กำไร/ขาดทุนของสถานะที่ยังไม่ปิด หน่วยเป็นเปอร์เซ็นต์
    # realized_profit คือ กำไร/ขาดทุนของสถานะที่ปิดไปแล้ว หน่วยเป็นบาท
    # โครงสร้าง portfolio -> {'entries':[{'symbol':...}, {'symbol':...}, {'symbol':...}]}
    # ทุกอิลิเม้นต์เป็น float
    # ยกเว้น symbol กับ position เป็น string
    #
    # 2) orders มี value เป็น list of dictionary โดยแต่ละ order มี key ทั้งหมด 11 อิลิเม้นต์: 'order_no', 'symbol', 'time',
    # 'position', 'price', 'volume', 'validity', 'status', 'position_action', 'acc_no', 'date'
    # โครงสร้าง orders -> [{'order_no':...}, {'order_no':...}, {'order_no':...}]
    # ทุกอิลิเม้นต์เป็น string ยกเว้น volume เป็น int
    # status หมายถึง order status มีค่าดังนี้ 'Matched', 'Queuing', 'Rejected'. 'Cancelled', 'Pending', 'Expired'
    # validity_type มีหลายค่า แต่ในระบบใช้ 'Day'
    # time มี format คือ HH:mm:ss
    # position มี 2 ค่า คือ 'Long', 'Short'
    # position_action มี 2 ค่า คือ 'Open', 'Close'
    # date มี format คือ dd/MM/yy
    # acc_no คือ derivatives account no. (เลขบัญชีพอร์ต TFEX)
    #
    # 3) account_info เป็น dictionary มี key ทั้งหมด 4 อิลิเม้นต์ : 'unrealized_profit', 'realized_profit', 'excess_equity', 'equity'
    # ทุกอิลิเม้นต์เป็นชนิด float
    def prepare_execution(self, trade_input, account_id):
        try:
            # Login and get derivatives - full portfolio info (data type is dictionary)
            # the type of output after calling is JSON
            # key is 'token', 'portfolio', 'orders', 'account_summary'

            queue_name = 'set_rpc_queue'
            request_action = 'get_drvt_full_portfolio_info'
            rpc_client = mq_client.RPCClient(self.__config['mq_host'], self.__config['mq_port'], queue_name)

            # Build request message
            rpc_client.req_message['request_header']['request_action'] = request_action
            rpc_client.req_message['request_header']['robot_name'] = self.__config['robot_name']
            rpc_client.req_message['request_header']['account_id'] = account_id
            rpc_client.req_message['request_body']['account_id'] = account_id


            # Create RPCClient object, encode message to JSON and call RPCClient to send message to message queue service
            req_message_json = json_util.encode(rpc_client.req_message)
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self.__config['robot_name'], 'INFO' \
                        , "JSON payload to send to execution system for getting derivatives-full portfolio info: " + str(req_message_json))
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self.__config['robot_name'], 'INFO' \
                        , " [x] Requesting get derivatives-full portfolio info")
            response_json = rpc_client.call(req_message_json)
            logger.info("%s: %s", self.__config['robot_name'],
                        "response_json = {}".format(response_json))

            # Convert JSON response to dictionary and set to trade_input
            portfolio_dict = json_util.decode(str(response_json))
            logger.debug('%s - %s - [%s][%s]: %s' \
                       , datetime_util.bangkok_now(), __name__ \
                       , self.__config['robot_name'], 'DEBUG' \
                       , "portfolio_dict = {}".format(portfolio_dict))
            trade_input.portfolio = portfolio_dict['response_body']['response_message']['portfolio']
            trade_input.orders = portfolio_dict['response_body']['response_message']['orders']
            trade_input.account_info = portfolio_dict['response_body']['response_message']['account_info']

            trade_input.cur_trade_portfolio_entry = self.__get_cur_trade_portfolio_entry(\
                trade_input.portfolio, trade_input.symbol)

        except Exception as e:
            raise err.TradeModelError("BaseExecutionModel.prepare_execution error: {}".format(e))

    # controller ทำหน้าที่รับผิดชอบการทำงานภายใน ExecutionModel ทั้งหมด
    # Parameter: trade_input - TradeInput
    # Param: trade_action - ForexTradeAction
    # Param: account_id - TFEX account id in string
    # Return: response_msg - Response message string from execution system
    def execute(self, trade_input, trade_action, account_id):
        #trade_output = trade_dto.TradeOutput()

        try:
            queue_name = 'set_rpc_queue'
            request_action = 'execute_trade'
            rpc_client = mq_client.RPCClient(self.__config['mq_host'], self.__config['mq_port'], queue_name)
            logger.debug('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self.__config['robot_name'], 'DEBUG' \
                        , "Create RPCClient object successfully")

            # =============================================================================================================
            # 1) Prepare request message
            # =============================================================================================================
            #trade_action.volume = 1
            trade_action_dict = trade_action.to_dict()

            # Build request message
            rpc_client.req_message['request_header']['request_action'] = request_action
            rpc_client.req_message['request_header']['robot_name'] = self.__config['robot_name']
            rpc_client.req_message['request_header']['account_id'] = account_id
            rpc_client.req_message['request_body']['account_id'] = account_id
            rpc_client.req_message['request_body']['trade_action'] = trade_action_dict

            # Create RPCClient object, encode message to JSON and call RPCClient to send message to message queue service
            req_message_json = json_util.encode(rpc_client.req_message)
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self.__config['robot_name'], 'INFO' \
                        , "JSON payload to send to execution system: " + str(req_message_json))
            logger.info('%s - %s - [%s][%s]: %s' \
                        , datetime_util.bangkok_now(), __name__ \
                        , self.__config['robot_name'], 'INFO' \
                        , " [x] Sending trade execution")

            # =============================================================================================================
            # 2) Send request message to message queue server
            # =============================================================================================================
            try:
                response_json = rpc_client.call(req_message_json)
                logger.info("%s: %s", self.__config['robot_name'],
                            "response_json = {}".format(response_json))

                # Convert JSON response to dictionary and set to trade_input
                exec_response_dict = json_util.decode(str(response_json))
                logger.info('%s - %s - [%s][%s]: %s' \
                            , datetime_util.bangkok_now(), __name__ \
                            , self.__config['robot_name'], 'INFO' \
                            , "exec_response_dict = {}".format(exec_response_dict))

                logger.info("%s: %s", self.__config['robot_name'],
                            "Call SET execution via message queue service successfully")

                response_code = exec_response_dict['response_header']['response_code']
                if response_code == 200:
                    response_message = 'Execution success: {}, {}'.format(self.__config['robot_name'], account_id)
                    logger.info('%s - %s - [%s][%s]: %s' \
                                , datetime_util.bangkok_now(), __name__ \
                                , self.__config['robot_name'], 'INFO' \
                                , "Execution success: {}".format(datetime_util.bangkok_now()))
                elif response_code == 500:
                    response_message = 'Execution error: {}, {}'.format(self.__config['robot_name'], account_id)
                    logger.info('%s - %s - [%s][%s]: %s' \
                                , datetime_util.bangkok_now(), __name__ \
                                , self.__config['robot_name'], 'INFO' \
                                , "Execution error: {}".format(datetime_util.bangkok_now()))
            except:
                logger.error('%s - %s - [%s][%s]: %s' \
                            , datetime_util.bangkok_now(), __name__ \
                            , self.__config['robot_name'], 'ERROR' \
                            , "Execution error: {}".format(datetime_util.bangkok_now()))
                raise Exception('Execution error: {}'.format(self.__config['robot_name']))

        except Exception as e:
            raise err.TradeModelError("BaseExecutionModel.execute error: {}".format(e))

        return response_message

    def __get_cur_trade_portfolio_entry(self, portfolio, symbol):
        # Set default entry
        cur_trade_portfolio_entry = {'symbol':symbol, 'position':''}

        # Get current trading position entry (dictionary) of specified symbol
        entries = portfolio['entries']
        if entries is not None and len(entries) > 0:
            for i in range(0, len(entries)):
                entry = entries[i]

                if entry['symbol'].lower() == symbol.lower() and entry['actual_pos_size'] > 0:
                    cur_trade_portfolio_entry = entry
                    break

        return cur_trade_portfolio_entry
# ==============================================================================================================
# END: Execution Model
# ==============================================================================================================