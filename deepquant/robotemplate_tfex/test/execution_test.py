import deepquant.common.json_util as json_util
import deepquant.common.mq_client as mq_client
import deepquant.common.state_machine as state_machine


mq_host = 'xxx.xxx.xxx.xxx'
mq_port = 28004

def get_cur_trade_portfolio_entry(portfolio, symbol):
    # Set default entry
    cur_trade_portfolio_entry = {'symbol': symbol, 'position': ''}

    # Get current trading position entry (dictionary) of specified symbol
    entries = portfolio['entries']
    if entries != None and len(entries) > 0:
        for i in range(0, len(entries)):
            entry = entries[i]

            if entry['symbol'].lower() == symbol.lower() and entry['actual_pos_size'] > 0:
                cur_trade_portfolio_entry = entry
                break

    return cur_trade_portfolio_entry


def test_prepare_execution(trade_input, account_id, symbol):
    try:
        # Login and get derivatives - full portfolio info (data type is dictionary)
        # the type of output after calling is JSON
        # key is 'token', 'portfolio', 'orders', 'account_summary'

        queue_name = 'set_rpc_queue'
        request_action = 'get_drvt_full_portfolio_info'
        rpc_client = mq_client.RPCClient(mq_host, mq_port, queue_name)

        # Build request message
        rpc_client.req_message['request_header']['request_action'] = request_action
        rpc_client.req_message['request_body']['account_id'] = account_id

        # Create RPCClient object, encode message to JSON and call RPCClient to send message to message queue service
        req_message_json = json_util.encode(rpc_client.req_message)
        print(" [x] Requesting get derivatives-full portfolio info")
        response_json = rpc_client.call(req_message_json)
        print("response_json = {}".format(response_json))

        # Convert JSON response to dictionary and set to trade_input
        portfolio_dict = json_util.decode(str(response_json))
        print("portfolio_dict = {}".format(portfolio_dict))
        portfolio = portfolio_dict['response_body']['response_message']['portfolio']
        print(portfolio)

        cur_trade_portfolio_entry = get_cur_trade_portfolio_entry(portfolio, symbol)
        print(cur_trade_portfolio_entry)

        st_machine = state_machine.StateMachine()

        cur_trade_position_str = cur_trade_portfolio_entry['position']
        cur_trade_position = st_machine.decode_state(cur_trade_position_str)
        signal_code = st_machine.SIGNAL_NONE

        action_code = st_machine.get_trade_action(signal_code, cur_trade_position)
        print('action code = {}'.format(action_code))

    except Exception as e:
        print("prepare_execution error: {}".format(e))
        raise


trade_input = None
account_id = 'xxxx'
symbol = 'S50Z18'
test_prepare_execution(trade_input, account_id, symbol)