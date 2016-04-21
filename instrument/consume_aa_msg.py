#!/usr/bin/env python
"""
@brief This script will consume messages that are published to the qpid queue
on the server from the command line with the routing key of oms.alertalarm.msg.
It will log the messages to the screen and the log file.

Usage:
   consume_aa <pqid_server> 

Options:
    -h, --help          Show this screen.

"""

import json
import pika
from docopt import docopt
from mi.core.log import LoggerManager
from ooi.logging import log

LoggerManager()
options = docopt(__doc__)
pqid_server = options['<pqid_server>']

connection = pika.BlockingConnection(pika.ConnectionParameters(host=pqid_server))
channel = connection.channel()

channel.exchange_declare(exchange='amq.topic', type='topic')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='amq.topic',
                   queue=queue_name,
                   routing_key='oms.alertalarm.msg')

def callback(ch, method, properties, body):
    log_str = 'consume_aa_msg: \n  ' + str(method.routing_key) + ':'

    body = json.loads(body)
    for body_item_key, body_item_val in body.iteritems():
        if body_item_key == 'attributes':
            log_str += '\n\tattributes :'
            for att_item_key, att_item_val in body_item_val.iteritems():
                log_str += '\n\t\t' + str(att_item_key) + ' : ' + str(att_item_val)
        else:
            log_str += '\n\t' + str(body_item_key) + ' : ' + str(body_item_val)

    log.info('%s',log_str)

channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

print('Consuming and logging Alert & Alarm Messages. To exit press CTRL+C')

channel.start_consuming()
