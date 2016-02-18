import json
import pprint
import sys
import logging
from kombu import BrokerConnection
from kombu import Exchange
from kombu import Queue
from kombu.mixins import ConsumerMixin

BROKER_URI = "amqp://guest:guest@localhost:5672//"

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class Message(object):
    def __init__(self, body):
        if not isinstance(body, dict):
            body = json.loads(body)

        if 'oslo.message' in body:
            self.v2 = True
            payload = body['oslo.message']
            if not isinstance(payload, dict):
                payload = json.loads(payload)
        else:
            self.v2 = False
            payload = body

        self.payload = payload
        self.event_type = payload.get('event_type', 'unknown')


class NotificationsDump(ConsumerMixin):
    exchange_names = ("nova", "openstack")
    routing_key = "notifications.info"
    queue_name = "tagging_queue"

    def __init__(self, connection):
        self.connection = connection
        return

    def get_consumers(self, consumer, channel):
        consumers = []
        for e in self.exchange_names:
            exchange = Exchange(e, type="topic", durable=False)
            queue = Queue(
                self.queue_name, exchange, routing_key=self.routing_key,
                durable=False, auto_delete=True, no_ack=True
            )
            consumers.append(consumer(queue, callbacks=[self.on_message]))
        return consumers

    def on_message(self, body, message):
        m = Message(body)
        logging.info("Event type: {0}".format(m.event_type))
        logging.info("V2: {0}".format(m.v2))
        logging.info("Body:\n{0}".format(pprint.pformat(m.payload)))
        logging.info("\n--------------------------")


if __name__ == '__main__':
    logging.info("Connecting to broker {}".format(BROKER_URI))
    with BrokerConnection(BROKER_URI) as connection:
        NotificationsDump(connection).run()
