# encoding: utf-8
"""
publisher.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "24 Feb 2025"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from datetime import datetime
import json
import logging
from typing import Dict

import pika
from pika.exceptions import AMQPConnectionError, UnroutableError, ChannelWrongStateError
from retry import retry

import config as CFG
import rabbit.routing_keys as RK

logger = logging.getLogger(RK.ADMIN)


class RabbitRetryError(BaseException):

    def __init__(self, *args: object, ampq_exception: Exception = None) -> None:
        super().__init__(*args)
        self.ampq_exception = ampq_exception


class RabbitMQPublisher:

    def __init__(self, name: str = "publisher"):
        # Get rabbit-specific section of config file
        self.whole_config = CFG.load_config()
        self.config = self.whole_config[CFG.RABBIT_CONFIG_SECTION]

        # Set name for logging purposes
        self.name = name

        # Load exchange section of config as this is the only required part for
        # sending messages
        self.exchanges = self.config["exchange"]

        # If multiple exchanges given then verify each and assign the first as a
        # default exchange.
        if not isinstance(self.exchanges, list):
            self.exchanges = [self.exchanges]
        for exchange in self.exchanges:
            self._verify_exchange(exchange)
        self.default_exchange = self.exchanges[0]

        self.connection = None
        self.channel = None
        self.heartbeat = self.config.get(CFG.RABBIT_CONFIG_HEARTBEAT) or 300
        self.timeout = self.config.get(CFG.RABBIT_CONFIG_TIMEOUT) or 1800  # 30 mins

    @staticmethod
    def _verify_exchange(exchange: str):
        """Verify that an exchange dict defined in the config file is valid.
        Throws a ValueError if not.

        """
        if (
            "name" not in exchange
            or "type" not in exchange
            or "delayed" not in exchange
        ):
            raise ValueError(
                "Exchange in config file incomplete, cannot " "be declared."
            )

    def declare_bindings(self):
        raise NotImplementedError

    @retry(RabbitRetryError, tries=-1, delay=1, backoff=2, max_delay=60, logger=logger)
    def get_connection(self):
        try:
            if not self.channel or not self.channel.is_open:
                # Get the username and password for rabbit
                rabbit_user = self.config["user"]
                rabbit_password = self.config["password"]

                # Start the rabbitMQ connection
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        self.config["server"],
                        credentials=pika.PlainCredentials(rabbit_user, rabbit_password),
                        virtual_host=self.config["vhost"],
                        heartbeat=self.heartbeat,
                        blocked_connection_timeout=self.timeout,
                    )
                )

                # Create a new channel with basic qos
                channel = connection.channel()
                channel.basic_qos(prefetch_count=1)
                channel.confirm_delivery()

                self.connection = connection
                self.channel = channel

                # Declare the exchange config. Also provides a hook for other
                # bindings (e.g. queues) to be declared in child classes.
                self.declare_bindings()
        except (AMQPConnectionError, ChannelWrongStateError) as e:
            logger.error(
                "AMQPConnectionError encountered on attempting to "
                "establish a connection. Retrying..."
            )
            logger.debug(f"{type(e).__name__}: {e}")
            raise RabbitRetryError(str(e), ampq_exception=e)

    def _get_default_properties(self, delay: int = 0) -> pika.BasicProperties:
        return pika.BasicProperties(
            content_encoding="application/json",
            headers={"x-delay": delay},
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
        )

    @retry(RabbitRetryError, tries=-1, delay=1, backoff=2, max_delay=60, logger=logger)
    def publish_message(
        self,
        routing_key: str,
        msg_dict: Dict,
        exchange: Dict = None,
        delay: int = 0,
        properties: pika.BasicProperties = None,
        mandatory_fl: bool = True,
        correlation_id: str = None,
    ) -> None:
        """Sends a message with the specified routing key to an exchange for
        routing. If no exchange is provided it will default to the first
        exchange declared in the server_config.

        An optional delay can be added which will force the message to sit for
        the specified number of seconds at the exchange before being routed.
        Note that this only happens if the given (or default if not specified)
        exchange is declared as a x-delayed-message exchange at start up with
        the 'delayed' flag.

        This is in essence a light wrapper around the basic_publish method in
        pika.
        """
        # add the time stamp to the message here
        msg_dict["timestamp"] = datetime.now().isoformat(sep="-")
        # JSON the message
        msg = json.dumps(msg_dict)

        if not exchange:
            exchange = self.default_exchange
        if not properties:
            properties = self._get_default_properties(delay=delay)
        if delay > 0:
            # Delayed messages and mandatory acknowledgements are unfortunately
            # incompatible. For now prioritising delay over the mandatory flag.
            mandatory_fl = False

        if correlation_id:
            properties.correlation_id = correlation_id

        try:
            self.channel.basic_publish(
                exchange=exchange["name"],
                routing_key=routing_key,
                properties=properties,
                body=msg,
                mandatory=mandatory_fl,
            )
        except (AMQPConnectionError, ChannelWrongStateError) as e:
            # For any connection error then reset the connection and try again
            logger.error(
                "AMQPConnectionError encountered on attempting to "
                "publish a message. Manually resetting and retrying."
            )
            logger.debug(f"{e}")
            self.connection = None
            self.get_connection()
            raise RabbitRetryError(str(e), ampq_exception=e)
        except UnroutableError as e:
            # For any Undelivered messages attempt to send again
            logger.error(
                "Message delivery was not confirmed, wasn't delivered "
                f"properly (rk = {routing_key})."
            )
            logger.debug(f"{type(e).__name__}: {e}")
            # NOTE: don't reraise in this case, can cause an infinite loop as
            # the message will never be sent.
            # raise RabbitRetryError(str(e), ampq_exception=e)

    def close_connection(self) -> None:
        self.connection.close()
