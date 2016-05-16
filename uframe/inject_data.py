#!/usr/bin/env python
import json
import time


def reformat(particle):
    external_keys = ['preferred_timestamp',
                     'port_timestamp',
                     'internal_timestamp',
                     'driver_timestamp']

    dropped_keys = ['time']

    new_particle = {}
    pk = particle.pop('pk', {})

    new_particle['stream_name'] = pk.get('stream')
    new_particle['quality_flag'] = 'ok'
    new_particle['pkt_format_id'] = 'JSON_Data'
    new_particle['pkt_version'] = 1

    for key in dropped_keys:
        particle.pop(key)

    for key in external_keys:
        value = particle.pop(key, 0)
        if value is not None:
            new_particle[key] = value

    for key in particle:
        new_particle.setdefault('values', []).append({'value_id': key, 'value': particle[key]})

    return {'type': 'DRIVER_ASYNC_EVENT_SAMPLE', 'value': new_particle, 'time': time.time()}


def kpublish(url, queue_name, refdes, particles):
    from kombu import Connection, Exchange, Producer, Queue
    headers = {'sensor': refdes, 'deliveryType': 'streamed'}
    with Connection(url) as conn:
        exchange = Exchange('amq.direct', type='direct')
        queue = Queue(name=queue_name, exchange=exchange, routing_key=queue_name)
        producer = Producer(conn, exchange=exchange, routing_key=queue_name)
        producer.publish(json.dumps(particles), content_encoding='ascii', content_type='text/plain',
                         headers=headers, declare=[queue], user_id='guest')


def qpublish(url, queue_name, refdes, particles):
    import qpid.messaging as qm
    headers = {'sensor': refdes, 'deliveryType': 'streamed'}
    conn = qm.Connection(url, username='guest', password='guest')
    conn.open()
    session = conn.session()
    sender = session.sender('%s; {create: always, node: {type: queue, durable: true}}' % queue_name)
    message = qm.Message(content=json.dumps(particles), content_type='text/plain', durable=True,
                         properties=headers, user_id='guest')
    sender.send(message, sync=True)


def load(path, url, queue, refdes):
    input_data = json.load(open(path))
    output_data = [reformat(p) for p in input_data]
    if 'qpid' in url:
        qpublish(url, queue, refdes, output_data)
    else:
        kpublish(url, queue, refdes, output_data)


def usage():
    print 'inject_data.py <json file> <qpid url> <queue name> <reference designator>'
    print 'e.g. inject_data.py data.json qpid://localhost Ingest.instrument_particles RS10ENGC-XX0XX-00-TESTDD001'


def main():
    import sys
    load(*sys.argv[1:])


if __name__ == '__main__':
    main()