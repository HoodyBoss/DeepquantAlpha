import pandas as pd

import deepquant.backtest.tradestate_context as state_ctx
import tradingrobot.eurusd_scalp.robot_config as robot_config


#import deepquant.market_fx_mt4.trade_dto as trade_dto
#import deepquant.tradingrobot_template.future1.test.robot_controller_test as robot_ctrl_test


class IndividualBackTest:

    #FOREX
    __point_value = 100

    #TFEX - S50
    #__point_value = 200

    def __int__(self, strategy):
        self.robot_config = robot_config.RobotConfig()
        self.tradestate_ctx = state_ctx.TradeStateContext()
        self.strategy_backtest = strategy

    """"
    def cal_avg_cost():
    def cal_equity():
    def cal_profit_with_chk_default_stoploss():
    def cal_profit_with_chk_trailing_stoploss():
    def cal_pos_size():
    def get_trailing_stoploss():
    """

    def stats_mark_stoploss_found(self, stoploss_type):
        stoploss_found = 0
        if (not (stoploss_type == 0)):
            stoploss_found = 1

        return stoploss_found

    def stats_loss_point(self, profitloss_point):
        loss_point = 0.0
        if (profitloss_point < 0):
            loss_point = profitloss_point

        return loss_point

    def stats_loss_money(self, loss_point, point_value):
        loss_money = 0.0
        if (loss_point < 0):
            loss_money = loss_point * point_value

        return loss_money

    """
    def build_trade_input(self, row):
        # 1) Build current trading position
        cur_trade_position = 'xxxx'

        # 2) Build portfolio
        portfolio = 'xxxx'

        # 3) Build account info
        account_info = 'xxxx'

        # 4) Assemble
        trade_input = trade_dto.TradeInput(cur_trade_position, portfolio, account_info)

        return trade_input
    """

    # Backtest trading system and returns test result in JSON format
    def backtest(self):

        # The dataset file must contains date time, OHLC(V), indicators and features
        asset_dataset = pd.read_csv("/EURUSD_M10_Dataset.csv")
        self.strategy_backtest.set_dataset(asset_dataset)

        max_row = len(asset_dataset.index)
        start_row = self.robot_config.max_backward_row
        end_row = max_row - 1

        # Create trade detailed log using pandas DataFrame
        detailed_log = pd.DataFrame()

        # Create trade list using pandas DataFrame
        trade_list = pd.DataFrame()

        for row in range(start_row, end_row):
            prev_trade_action = ""
            prev_avg_cost = 0.0
            prev_stoploss = 0.0
            prev_equity = 0.0
            prev_pos_size = 0.0

            # Simulate current environment state
            # self.simulate_env()

            # Build trade input
            trade_input = self.build_trade_input(row)

            # Get trading action
            trade_action = self.strategy_backtest.run_trade(trade_input)

            # Get avg. cost, equity, profit/loss pip, position size,
            # trailing stop loss position
            avg_cost            = self.cal_avg_cost()
            equity              = self.cal_equity()
            profitloss_pip      = self.cal_profit_with_chk_trailing_stoploss()
            pos_size            = self.cal_pos_size()
            trailing_stoploss   = self.get_trailing_stoploss()

            #======================================================================================
            # BEGIN: Statistics Matrix
            #======================================================================================
            # Get detect stop loss type
            stoploss_type = self.stats_detect_trailing_stoploss()

            # Value 1 -> if found stop loss, value 0 -> not found
            stoploss_found = self.stats_mark_stoploss_found(stoploss_type)

            # Get loss in point
            loss_point = self.stats_loss_point(profitloss_pip)

            # Get loss in money
            loss_money = self.stats_loss_money(loss_point, self.__point_value)

            #======================================================================================
            # END: Statistics Matrix
            #======================================================================================

            # Insert new result
            self.tradestate_ctx.add_state(trade_action, trailing_stoploss,
                                                     pos_size, avg_cost, equity, profitloss_pip, stoploss_type,
                                                     stoploss_found, loss_point, loss_money)

        return self.tradestate_ctx.get_tradestate_context().to_json(orient='records')