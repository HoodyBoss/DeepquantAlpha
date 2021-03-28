import pandas

class TradeStateContext:

    __result_columns = ['trade_action', 'stop_loss', 'pos_size', 'avg_cost',
                        'equity', 'profit_pip', 'stoploss_type', 'stoploss_num',
                        'loss_num', 'loss']


    def __init__(self):
        self.context = pandas.DataFrame(columns=self.__result_columns)


    # Add new trading state
    def add_state(self, trade_action, stoploss,
        pos_size, avg_cost, equity, profit_pip, stoploss_type,
        stoploss_num, loss_num, loss):

        self.context.loc[len(self.context.index)] = [trade_action, stoploss,
                                                   pos_size, avg_cost, equity, profit_pip, stoploss_type,
                                                   stoploss_num, loss_num, loss]


    # Returns trading state context containing only latest N trading state in memory
    def get_tradestate_context(self):
        return self.context


    # Returns latest trading state from trading state context
    def get_tradestate_last_row(self):
        row_num = len(self.context)
        return self.context.iloc[row_num - 1:]