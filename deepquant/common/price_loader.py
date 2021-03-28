import deepquant.common.json_util as json_util
import deepquant.common.mq_client as mq_client
import deepquant.common.error as err


class PriceLoader:

    def __init__(self, mq_host, mq_port):
        self.__mq_host = mq_host
        self.__mq_port = mq_port

    def get_price(self, symbol, timeframe, output_format):
        """
        Returns price dictionary: datetime, open, high, low, close, volume
        Datetime format is dd/MM/yyyy HH:mm:ss
        Datetime is bar time
        :param symbol:
        :param timeframe: string -> '5' for 5 minutes, '15' for 15 minutes
        :return:
        """
        try:
            queue_name = 'set_rpc_queue'
            request_action = 'get_price'
            rpc_client = mq_client.RPCClient(self.__mq_host, self.__mq_port, queue_name)

            # Build request message
            rpc_client.req_message['request_header']['request_action'] = request_action
            rpc_client.req_message['request_body']['symbol'] = symbol
            rpc_client.req_message['request_body']['timeframe'] = timeframe
            rpc_client.req_message['request_body']['output_format'] = output_format

            # Create RPCClient object, encode message to JSON and call RPCClient to send message to message queue service
            req_message_json = json_util.encode(rpc_client.req_message)
            response_json = rpc_client.call(req_message_json)

            # Convert JSON response to dictionary and set to trade_input
            response_dict = json_util.decode(str(response_json))
            price_dict = response_dict['response_body']['response_message']

        except Exception as e:
            raise err.DeepQuantError("PriceLoader.get_price error: {}".format(e))

        return price_dict
