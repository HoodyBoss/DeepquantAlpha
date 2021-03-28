

class State:

    STATE_BUY               = 1
    STATE_SELL              = 2
    STATE_IDLE              = 3
    STATE_WAIT_FOR_BUY      = 4
    STATE_WAIT_FOR_SELL     = 5

    def __init__(self, name, signal_action_mapping):
        self.name = name
        self.signal_action_mapping = signal_action_mapping

    def equals(self, compare_state):
        result = False

        if self.name == compare_state.name:
            result = True

        return result


class StateMachine:

    #====================================================================================
    # Create trading signal
    SIGNAL_BUY                      = 1
    SIGNAL_SELL                     = 2
    SIGNAL_NONE                     = 3
    SIGNAL_CLOSE_AND_WAIT           = 4
    SIGNAL_CLOSE_AND_WAIT_FOR_BUY   = 5
    SIGNAL_CLOSE_AND_WAIT_FOR_SELL  = 6
    SIGNAL_SCALE_IN                 = 7
    SIGNAL_SCALE_OUT                = 8
    SIGNAL_ROLLOVER                 = 9
    SIGNAL_MODIFY_STOP_LOSS         = 10
    SIGNAL_MODIFY_TAKE_PROFIT       = 11


    #====================================================================================
    # Create trading action
    ACTION_OPEN_BUY                 = 1
    ACTION_OPEN_SELL                = 2
    ACTION_HOLD_BUY                 = 3
    ACTION_HOLD_SELL                = 4
    ACTION_CLOSE_BUY                = 5
    ACTION_CLOSE_SELL               = 6
    ACTION_WAIT                     = 7
    ACTION_WAIT_FOR_BUY             = 8
    ACTION_WAIT_FOR_SELL            = 9
    ACTION_CLOSE_BUY_WAIT           = 10
    ACTION_CLOSE_SELL_WAIT          = 11
    ACTION_CLOSE_BUY_WAIT_FOR_BUY   = 12
    ACTION_CLOSE_BUY_WAIT_FOR_SELL  = 13
    ACTION_CLOSE_SELL_WAIT_FOR_SELL = 14
    ACTION_CLOSE_SELL_WAIT_FOR_BUY  = 15
    ACTION_SCALE_IN_BUY             = 16
    ACTION_SCALE_OUT_BUY            = 17
    ACTION_SCALE_IN_SELL            = 18
    ACTION_SCALE_OUT_SELL           = 19
    ACTION_ROLLOVER_BUY             = 20
    ACTION_ROLLOVER_SELL            = 21
    ACTION_MODIFY_POSITION          = 22


    #===================================================================================
    # Create trading signal and action mapping
    # signal and action for BUY state (trading position)
    buy_sig_act_mapping = { SIGNAL_BUY                      : ACTION_HOLD_BUY,
                            SIGNAL_SELL                     : ACTION_OPEN_SELL,
                            SIGNAL_NONE                     : ACTION_HOLD_BUY,
                            SIGNAL_CLOSE_AND_WAIT           : ACTION_CLOSE_BUY_WAIT,
                            SIGNAL_CLOSE_AND_WAIT_FOR_BUY   : ACTION_CLOSE_BUY_WAIT_FOR_BUY,
                            SIGNAL_CLOSE_AND_WAIT_FOR_SELL  : ACTION_CLOSE_BUY_WAIT_FOR_SELL,
                            SIGNAL_SCALE_IN                 : ACTION_SCALE_IN_BUY,
                            SIGNAL_SCALE_OUT                : ACTION_SCALE_OUT_BUY,
                            SIGNAL_ROLLOVER                 : ACTION_ROLLOVER_BUY,
                            SIGNAL_MODIFY_STOP_LOSS         : ACTION_MODIFY_POSITION,
                            SIGNAL_MODIFY_TAKE_PROFIT       : ACTION_MODIFY_POSITION }

    # signal and action for SELL state (trading position)
    sell_sig_act_mapping = {SIGNAL_SELL                     : ACTION_HOLD_SELL,
                            SIGNAL_BUY                      : ACTION_OPEN_BUY,
                            SIGNAL_NONE                     : ACTION_HOLD_SELL,
                            SIGNAL_CLOSE_AND_WAIT           : ACTION_CLOSE_SELL_WAIT,
                            SIGNAL_CLOSE_AND_WAIT_FOR_BUY   : ACTION_CLOSE_SELL_WAIT_FOR_BUY,
                            SIGNAL_CLOSE_AND_WAIT_FOR_SELL  : ACTION_CLOSE_SELL_WAIT_FOR_SELL,
                            SIGNAL_SCALE_IN                 : ACTION_SCALE_IN_SELL,
                            SIGNAL_SCALE_OUT                : ACTION_SCALE_OUT_SELL,
                            SIGNAL_ROLLOVER                 : ACTION_ROLLOVER_SELL,
                            SIGNAL_MODIFY_STOP_LOSS         : ACTION_MODIFY_POSITION,
                            SIGNAL_MODIFY_TAKE_PROFIT       : ACTION_MODIFY_POSITION }

    # signal and action for IDLE state (trading position)
    idle_sig_act_mapping        = { SIGNAL_BUY                      : ACTION_OPEN_BUY,
                                    SIGNAL_SELL                     : ACTION_OPEN_SELL,
                                    SIGNAL_NONE                     : ACTION_WAIT,
                                    SIGNAL_CLOSE_AND_WAIT           : ACTION_WAIT,
                                    SIGNAL_CLOSE_AND_WAIT_FOR_BUY   : ACTION_WAIT,
                                    SIGNAL_CLOSE_AND_WAIT_FOR_SELL  : ACTION_WAIT,
                                    SIGNAL_SCALE_IN                 : ACTION_WAIT,
                                    SIGNAL_SCALE_OUT                : ACTION_WAIT }

    # signal and action for WAIT FOR BUY state (trading position)
    wait_buy_sig_act_mapping    = {SIGNAL_BUY                       : ACTION_OPEN_BUY,
                                   SIGNAL_SELL                      : ACTION_WAIT_FOR_BUY,
                                   SIGNAL_NONE                      : ACTION_WAIT_FOR_BUY,
                                   SIGNAL_CLOSE_AND_WAIT            : ACTION_WAIT,
                                   SIGNAL_CLOSE_AND_WAIT_FOR_BUY    : ACTION_WAIT,
                                   SIGNAL_CLOSE_AND_WAIT_FOR_SELL   : ACTION_WAIT,
                                   SIGNAL_SCALE_IN                  : ACTION_WAIT,
                                   SIGNAL_SCALE_OUT                 : ACTION_WAIT }

    # signal and action for WAIT FOR SELL state (trading position)
    wait_sell_sig_act_mapping   = {SIGNAL_SELL                      : ACTION_OPEN_SELL,
                                   SIGNAL_BUY                       : ACTION_WAIT_FOR_SELL,
                                   SIGNAL_NONE                      : ACTION_WAIT_FOR_SELL,
                                   SIGNAL_CLOSE_AND_WAIT            : ACTION_WAIT,
                                   SIGNAL_CLOSE_AND_WAIT_FOR_BUY    : ACTION_WAIT,
                                   SIGNAL_CLOSE_AND_WAIT_FOR_SELL   : ACTION_WAIT,
                                   SIGNAL_SCALE_IN                  : ACTION_WAIT,
                                   SIGNAL_SCALE_OUT                 : ACTION_WAIT }


    #====================================================================================
    # Create trading state
    buy_state       = State(State.STATE_BUY, buy_sig_act_mapping)
    sell_state      = State(State.STATE_SELL, sell_sig_act_mapping)
    idle_state      = State(State.STATE_IDLE, idle_sig_act_mapping)
    wait_buy_state  = State(State.STATE_WAIT_FOR_BUY, wait_buy_sig_act_mapping)
    wait_sell_state = State(State.STATE_WAIT_FOR_SELL, wait_sell_sig_act_mapping)


    #====================================================================================
    #def get_action_name(self, action_code):
    #    action_name = dict((v, k) for k, v in self.trade_action_dict.items()).get(action_code)
    #    return action_name

    """
    Returns trading action corresponding to trading signal and current trading position
    trade_signal - new trading signal
    cur_state - current trading position
    """
    def get_trade_action(self, trade_signal, cur_state):

        # Get trading signal and action mapping
        mapping = None
        if cur_state == State.STATE_BUY:
            mapping = self.buy_sig_act_mapping
        elif cur_state == State.STATE_SELL:
            mapping = self.sell_sig_act_mapping
        elif cur_state == State.STATE_IDLE:
            mapping = self.idle_sig_act_mapping
        elif cur_state == State.STATE_WAIT_FOR_BUY:
            mapping = self.wait_buy_sig_act_mapping
        elif cur_state == State.STATE_WAIT_FOR_SELL:
            mapping = self.wait_sell_sig_act_mapping

        trade_state = State(cur_state, mapping)                         # Create State object
        try:
            trade_action = trade_state.signal_action_mapping[trade_signal]  # Get trading action
        except KeyError:
            if cur_state == trade_state.STATE_BUY:
                trade_action.action_code = self.ACTION_HOLD_BUY
            elif cur_state == trade_state.STATE_SELL:
                trade_action.action_code = self.ACTION_HOLD_SELL
            elif cur_state == trade_state.STATE_IDLE:
                trade_action.action_code = self.ACTION_WAIT
            elif cur_state == trade_state.STATE_WAIT_FOR_BUY:
                trade_action.action_code = self.ACTION_WAIT_FOR_BUY
            elif cur_state == trade_state.STATE_WAIT_FOR_SELL:
                trade_action.action_code = self.ACTION_WAIT_FOR_SELL

        return trade_action

    # Decode state (or position) Long = Buy, Short = Sell
    def decode_state(self, state):
        output_state = State.STATE_IDLE

        if state.lower() == 'long' or state.lower() == 'buy':
            output_state = State.STATE_BUY
        elif state.lower() == 'short' or state.lower() == 'sell':
            output_state = State.STATE_SELL

        return output_state

    def get_signal_name(self, signal_code):
        switcher = {
            1 : 'BUY',
            2 : 'SELL',
            3 : 'NONE',
            4 : 'CLOSE AND WAIT',
            5 : 'CLOSE AND WAIT FOR BUY',
            6 : 'CLOSE AND WAIT FOR SELL',
            7 : 'SCALE IN',
            8 : 'SCALE OUT',
            9 : 'ROLLOVER',
            10 : 'MODIFY STOP LOSS',
            11 : 'MODIFY TAKE PROFIT'
        }
        return switcher.get(signal_code)

    def get_action_name(self, action_code):
        switcher = {
            1 : 'OPEN BUY',
            2 : 'OPEN SELL',
            3 : 'HOLD BUY',
            4 : 'HOLD SELL',
            5 : 'CLOSE BUY',
            6 : 'CLOSE SELL',
            7 : 'WAIT',
            8 : 'WAIT FOR BUY',
            9 : 'WAIT FOR SELL',
            10 : 'CLOSE BUY WAIT',
            11 : 'CLOSE SELL WAIT',
            12 : 'CLOSE BUY WAIT FOR BUY',
            13 : 'CLOSE BUY WAIT FOR SELL',
            14 : 'CLOSE SELL WAIT FOR SELL',
            15 : 'CLOSE SELL WAIT FOR BUY',
            16 : 'SCALE IN BUY',
            17 : 'SCALE OUT BUY',
            18 : 'SCALE IN SELL',
            19 : 'SCALE OUT SELL',
            20 : 'ROLLOVER BUY',
            21 : 'ROLLOVER SELL',
            22 : 'MODIFY POSITION'
        }
        return switcher.get(action_code)


