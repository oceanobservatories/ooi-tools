#!/usr/bin/env python
import sys
import logging
import qpid.messaging as qm

HOST = 'uframe'
PORT = 5672
USER = 'guest'

def get_logger():
    logger = logging.getLogger('driver_control')
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    return logger


log = get_logger()

def send(queue, content):
    response_queue = "#test_response_queue; {create: always, delete: always}"
    message = qm.Message(content=content, user_id=USER, reply_to=response_queue)
    conn = qm.Connection(host=HOST, port=PORT, username=USER, password=USER)
    conn.open()
    sender = conn.session().sender(queue)
    receiver = conn.session().receiver(response_queue)
    sender.send(message)
    print receiver.fetch()

if __name__ == '__main__':
    send(sys.argv[1], ' '.join(sys.argv[2:]))
