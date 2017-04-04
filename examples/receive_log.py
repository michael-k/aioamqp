#!/usr/bin/env python
"""
    Rabbitmq.com pub/sub example

    https://www.rabbitmq.com/tutorials/tutorial-three-python.html

"""

import asyncio
import aioamqp
import random


import logging
logging.basicConfig(level='DEBUG', format="%(levelname)s %(asctime)s %(name)s %(message)s")
from logilab.common.logging_ext import set_color_formatter
set_color_formatter()

def error_callback(exception):
    print(exception)


@asyncio.coroutine
def callback(channel, body, envelope, properties):
    print(" [x] %r" % body)


@asyncio.coroutine
def receive_log():
    try:
        transport, protocol = yield from aioamqp.connect('localhost', 5672, on_error=error_callback)
    except aioamqp.AmqpClosedConnection:
        print("closed connections")
        return


    channel = yield from protocol.channel()
    exchange_name = 'logs'

    yield from channel.exchange(exchange_name=exchange_name, type_name='fanout')

    # let RabbitMQ generate a random queue name
    result = yield from channel.queue(queue_name='', exclusive=True)

    queue_name = result['queue']
    yield from channel.queue_bind(exchange_name=exchange_name, queue_name=queue_name, routing_key='')

    print(' [*] Waiting for logs. To exit press CTRL+C')

    yield from channel.basic_consume(callback, queue_name=queue_name, no_ack=True)

event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(receive_log())
event_loop.run_forever()
