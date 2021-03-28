import pika
import uuid


class RPCClient(object):
    req_message = None

    def __init__(self, mq_host, mq_port, queue_name):
        self.req_message = {'request_header' : {'request_action' : ''},
                            'request_body' : {'account_id':''}}
        self.queue_name = queue_name

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_host, port=mq_port))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, req_message_str):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue_name,
                                   properties=pika.BasicProperties(
                                         reply_to = self.callback_queue,
                                         correlation_id = self.corr_id,
                                         ),
                                   body=req_message_str)

        while self.response is None:
            self.connection.process_data_events()

        self.connection.close()

        return str(self.response.decode('utf-8'))


class PublishSubscribeClient():

    #req_message = {'request_header' : {'robot_name' : '', 'account_id': ''},
    #               'request_body' : {'trade_action':{}}}

    req_message = None

    def __init__(self, mq_host, mq_port, queue_name):
        self.req_message = {'request_header' : {'request_action' : ''},
                            'request_body' : {'account_id':''}}
        self.queue_name = queue_name

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_host, port=mq_port))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=queue_name,
                                      exchange_type='fanout')

    def call(self, req_message_str):
        self.channel.basic_publish(exchange=self.queue_name,
                              routing_key='',
                              body=req_message_str)

        print(" [x] Sent %r" % req_message_str)
        self.connection.close()
