#!/usr/bin/env python
"""
@brief This script will watch and log the messages that are published to the
amp queue.  The queue watched is on the host passed in associated with the
exchange and the routing key passed in. It will log the messages to the screen
and to the log file.

Usage:
   watch_topic <host> <exchange> <routing_key>

Options:
    -h, --help          Show this screen.

"""

import pprint
import json
import pika
from docopt import docopt
from mi.core.log import LoggerManager
from ooi.logging import log

LoggerManager()
log.setLevel('INFO')


def callback(ch, method, properties, body):
    body = json.loads(body)
    log.info(str(method.routing_key) + '\n' + pprint.pformat(body))


def main():
    """
    This main routine will get the amq command line parameters for the topic
    to watch.  It will make the connection to the host, bind the queue to the
    exchange and routing key passed in and define the callback for logging.
    It will then listen for messages published to the queue and log them.
    """

    options = docopt(__doc__)
    host = options['<host>']
    exchange = options['<exchange>']
    routing_key = options['<routing_key>']

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    channel = connection.channel()
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange=exchange,
                       queue=queue_name,
                       routing_key=routing_key)

    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=True)

    log.info('Consuming and logging ' + routing_key + ' messages. ' +
             'To exit press CTRL+C')

    channel.start_consuming()

if __name__ == '__main__':
    main()
