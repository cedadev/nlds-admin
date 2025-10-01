# encoding: utf-8
"""
consumer.py
"""
__author__ = "Neil Massey"
__date__ = "15 Sep 2025"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import functools
import logging
import traceback
from typing import Dict, List, Any

import json

from pika.exceptions import StreamLostError, AMQPConnectionError
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method, Header
from pika.amqp_object import Method
from pika.spec import Channel

import nlds_admin.rabbit.routing_keys as RK
import nlds_admin.rabbit.message_keys as MSG
from nlds_admin.rabbit.state import State
from nlds_admin.rabbit.publisher import RabbitMQPublisher as RMQP
import nlds_admin.config as CFG

logger = logging.getLogger("nlds.root")


class RabbitQEBinding:
    def __init__(self, exchange: str, routing_key: str):
        self.exchange = exchange
        self.routing_key = routing_key

class RabbitQueue:
    def __init__(self, name: str, bindings: List[RabbitQEBinding]):
        self.name = name
        self.bindings = bindings

    @classmethod
    def from_defaults(cls, queue_name, exchange, routing_key):
        return cls(
            name=queue_name,
            bindings=[RabbitQEBinding(exchange=exchange, routing_key=routing_key)],
        )


class SigTermError(Exception):
    pass


def deserialize(body: str) -> dict:
    """Deserialize the message body by calling JSON loads and decompressing the
    message if necessary."""
    # NRM - in v1.0.11 of NLDS the data is compressed.  This function only currently
    # works for v1.0.9
    body_dict = json.loads(body)
    return body_dict

class RabbitMQConsumer(RMQP):

    def __setup_queues(self, queue: str = None):
        # TODO: (2021-12-21) Only one queue can be specified at the moment,
        # should be able to specify multiple queues to subscribe to but this
        # isn't a priority.
        self.name = queue
        try:
            if queue is not None:
                # If queue specified then select only that configuration
                if CFG.RABBIT_CONFIG_QUEUES in self.config:
                    # Load queue config if it exists in .server_config file.
                    self.queues = [
                        RabbitQueue(**q)
                        for q in self.config[CFG.RABBIT_CONFIG_QUEUES]
                        if q[CFG.RABBIT_CONFIG_QUEUE_NAME] == queue
                    ]
                else:
                    raise ValueError("No rabbit queues found in config.")

                if queue not in [q.name for q in self.queues]:
                    raise ValueError(f"Requested queue {queue} not in configuration.")

            else:
                raise ValueError("No queue specified, switching to default " "config.")

        except ValueError as e:
            raise Exception(e)

    def __init__(self, queue: str = None):
        super().__init__(name=queue)
        # don't loop in the admin, just run once and exit
        self.loop = False

        self.__setup_queues(queue)

        # Load consumer-specific config
        if self.name in self.whole_config:
            self.consumer_config = self.whole_config[self.name]
        else:
            raise RuntimeError(f"Config not found for consumer {self.name}")

        # List of active threads created by consumption process
        self.threads = []

    def send_pathlist(
        self,
        pathlist: List[str],
        routing_key: str,
        body_json: Dict[str, Any],
        state: State = None,
        warning: List[str] = None,
        delay=0,
    ) -> None:
        """Convenience function which sends the given list of PathDetails
        objects to the exchange with the given routing key and message body.
        Mode specifies what to put into the log message.

        Additionally forwards transaction state info on to the monitor. As part
        of this it keeps track of the number of messages sent and reassigns
        message sub_ids appropriately so that monitoring can keep track of the
        transaction's state more easily.

        """
        # monitoring routing
        # monitoring_rk = ".".join([routing_key.split(".")[0], RK.MONITOR_PUT, RK.START])
        # # shouldn't send empty pathlist
        # if len(pathlist) == 0:
        #     raise MessageError("Pathlist is empty")
        # # If necessary values not set at this point then use default values
        # if state is None:
        #     state = self.DEFAULT_STATE

        # # NRM 03/09/2025 - the sub_id is now the hash of the pathlist
        # c_sub_id = body_json[MSG.DETAILS][MSG.SUB_ID]
        # sub_id = self.create_sub_id(pathlist)
        # if sub_id != c_sub_id:
        #     self.log(
        #         f"Changing sub id from {c_sub_id} to {sub_id} with pathlist {pathlist}",
        #         RK.LOG_INFO,
        #     )
        #     # send a complete message for the old sub id, as it has been split into
        #     # sub messagews
        #     body_json[MSG.DETAILS][MSG.STATE] = state.SPLIT.value
        #     self.publish_message(monitoring_rk, body_json, delay=delay)
        #     # reassign the sub_id
        #     body_json[MSG.DETAILS][MSG.SUB_ID] = sub_id

        # # Send message to next part of workflow
        # body_json[MSG.DATA][MSG.FILELIST] = pathlist
        # body_json[MSG.DETAILS][MSG.STATE] = state.value

        # self.publish_message(routing_key, body_json, delay=delay)

        # # Send message to monitoring to keep track of state
        # # add any warning
        # if warning and len(warning) > 0:
        #     body_json[MSG.DETAILS][MSG.WARNING] = warning

        # # added the delay back in for the PREPARE method
        # self.publish_message(monitoring_rk, body_json, delay=delay)

    def send_complete(
        self,
        routing_key: str,
        body_json: Dict[str, Any],
    ):
        body_json[MSG.DETAILS][MSG.STATE] = State.COMPLETE
        monitoring_rk = ".".join([routing_key.split(".")[0], RK.MONITOR_PUT, RK.START])
        self.publish_message(monitoring_rk, body_json)


    @staticmethod
    def _acknowledge_message(channel: Channel, delivery_tag: str) -> None:
        """Acknowledge a message with a basic ack. This is the bare minimum
        requirement for an acknowledgement according to rabbit protocols.

        :param channel:         Channel which message came from
        :param delivery_tag:    Message id
        """

        logger.debug(f"Acknowledging message: {delivery_tag}")
        if channel.is_open:
            channel.basic_ack(delivery_tag)

    @staticmethod
    def _nacknowledge_message(channel: Channel, delivery_tag: str) -> None:
        """Nacknowledge a message with a basic nack.

        :param channel:         Channel which message came from
        :param delivery_tag:    Message id
        """

        logger.debug(f"Nacking message: {delivery_tag}")
        if channel.is_open:
            channel.basic_nack(delivery_tag=delivery_tag)

    def acknowledge_message(
        self, channel: Channel, delivery_tag: str, connection: Connection
    ) -> None:
        """Method for acknowledging a message so the next can be fetched. This
        should be called at the end of a consumer callback, and - in order to do
        so thread-safely - from within connection object.  All of the required
        params come from the standard callback params.

        :param channel:         Callback channel param
        :param delivery_tag:    From the callback method param. eg.
                                method.delivery_tag
        :param connection:      Connection object from the callback param
        """
        cb = functools.partial(self._acknowledge_message, channel, delivery_tag)
        connection.add_callback_threadsafe(cb)

    def nack_message(
        self, channel: Channel, delivery_tag: str, connection: Connection
    ) -> None:
        """Method for nacknowledging a message so that it can be requeued and
        the next can be fetched. This is called after a consumer callback if,
        and only if, it returns a False. As in the case of acking, in order to
        do this thread-safely it is done from within a connection object.  All
        of the required params come from the standard callback params.

        :param channel:         Callback channel param
        :param delivery_tag:    From the callback method param. eg.
                                method.delivery_tag
        :param connection:      Connection object from the callback param
        """
        cb = functools.partial(self._nacknowledge_message, channel, delivery_tag)
        connection.add_callback_threadsafe(cb)

    def _deserialize(self, body: bytes) -> dict[str, str]:
        """Deserialize the message body by calling JSON loads and decompressing the
        message if necessary."""
        return deserialize(body)

    def callback(
        self,
        ch: Channel,
        method: Method,
        properties: Header,
        body: bytes,
        connection: Connection,
    ) -> None:
        """Standard consumer callback function as defined by rabbitMQ, with the
        standard callback parameters of Channel, Method, Header, Body (in bytes)
        and Connection.

        Just ack or nack a message
        """
        ack_fl = False
        if ack_fl is False:
            self.nack_message(ch, method.delivery_tag, connection)
        else:
            self.acknowledge_message(ch, method.delivery_tag, connection)

    def declare_bindings(self) -> None:
        """
        Overridden method from Publisher, additionally declares the queues and
        queue-exchange bindings outlined in the config file. If no queues were
        set then the default - generated within __init__ - is used instead.

        """
        #super().declare_bindings()
        for queue in self.queues:
            self.channel.queue_declare(queue=queue.name, durable=True)
            for binding in queue.bindings:
                self.channel.queue_bind(
                    exchange=binding["exchange"],
                    queue=queue.name,
                    routing_key=binding["routing_key"],
                )
            # Apply callback to all queues
            wrapped_callback = functools.partial(
                self.callback, connection=self.connection
            )
            self.channel.basic_consume(
                queue=queue.name, on_message_callback=wrapped_callback
            )

    @staticmethod
    def split_routing_key(routing_key: str) -> None:
        """
        Method to simply verify and split the routing key into parts.

        :return: 3-tuple of routing key parts
        """
        rk_parts = routing_key.split(".")
        if len(rk_parts) != 3:
            raise ValueError(
                f"Routing key ({routing_key}) malformed, should " "consist of 3 parts."
            )
        return rk_parts

    @classmethod
    def append_route_info(cls, body: Dict[str, Any], route_info: str = None) -> Dict:
        if MSG.ROUTE in body[MSG.DETAILS]:
            body[MSG.DETAILS][MSG.ROUTE] += route_info
        else:
            body[MSG.DETAILS][MSG.ROUTE] = route_info
        return body


    def consume_one_message(self):
        """Method that consumes just one message """
        self.get_connection()
        try:
            x = self.channel.consume(self.name)
            # here a 3-tuple of (method, properties, body) is returned
            method, properties, body = next(x)

        except (StreamLostError, AMQPConnectionError) as e:
            # Log problem
            logger.error("Connection lost, reconnecting", exc_info=e)

        except Exception as e:
            # Catch all other exceptions and log them as critical.
            tb = traceback.format_exc()
            raise e
        return method, properties, body

    def close(self):
        self.channel.stop_consuming()
        self.connection.close()

    def basic_ack(self, method):
        self.channel.basic_ack(method.delivery_tag)


    def run(self):
        """
        Method to run when main is started. Creates an AMQP connection
        and sets some exception handling.

        A common exception which occurs is StreamLostError.
        The connection should get reset if that happens.

        :return:
        """
        self.get_connection()
        try:
            self.channel.start_consuming()

        except (StreamLostError, AMQPConnectionError) as e:
            # Log problem
            logger.error("Connection lost, reconnecting", exc_info=e)

        except Exception as e:
            # Catch all other exceptions and log them as critical.
            tb = traceback.format_exc()
            raise e

        self.channel.stop_consuming()

        self.connection.close()