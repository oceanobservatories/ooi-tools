#!/usr/bin/env python
import click
from dateutil.parser import parse
from uframe import SensorInventory, Fetcher, log, make_isoformat
from preload_database import database
from preload_database.model.preload import Stream
from multiprocessing.pool import ThreadPool


def validate(stream, parameters, fill, deployment=None, start=None, stop=None, limit=10):
    f = Fetcher(stream, deployment=deployment, start=start, stop=stop, limit=limit)
    return f.validate(parameters, fill)


def _initdb():
    database.initialize_connection(database.PreloadDatabaseMode.POPULATED_MEMORY)
    database.open_connection()


def filter_stream(found_stream, stream_filters):
    if not stream_filters:
        return True
    for each in stream_filters:
        if each in found_stream.stream:
            return True
    return False


@click.command()
@click.option('--subsites', default=None, help='One or more subsites to be queried', multiple=True)
@click.option('--nodes', default=None, help='One or more nodes to be queried', multiple=True)
@click.option('--sensors', default=None, help='One or more sensors to be queried', multiple=True)
@click.option('--methods', default=None, help='One or more methods to be queried', multiple=True)
@click.option('--streams', default=None, help='One or more streams to be queried', multiple=True)
@click.option('--start', default=None, help='Query start time')
@click.option('--stop', default=None, help='Query stop time')
@click.option('--deployment', default=None, type=int, help='Use deployment # for times')
@click.option('--limit', default=10, help='Number of particles to query')
def check_stream(subsites, nodes, sensors, methods, streams, start, stop, deployment, limit):
    pool = ThreadPool(10)
    if start:
        start = make_isoformat(parse(start))
    if stop:
        stop = make_isoformat(parse(stop))
    inv = SensorInventory()
    found_streams = inv.get_streams(subsites=subsites, nodes=nodes, sensors=sensors, methods=methods)
    found_streams = [s for s in found_streams if filter_stream(s, streams)]

    futures = []
    for stream in found_streams:
        preload_stream = Stream.query.filter(Stream.name == stream.stream).first()
        parameters = [p.name for p in preload_stream.parameters]
        fill = {}
        for p in preload_stream.parameters:
            try:
                fill[p.name] = float(p.fill_value.value)
                fill[p.name] = int(p.fill_value.value)
            except (ValueError, AttributeError):
                fill[p.name] = None
        futures.append(pool.apply_async(validate, (stream, parameters, fill),
                                        {'start': start, 'stop': stop, 'deployment': deployment, 'limit': limit}))

    for future in futures:
        stream, missing, filled = future.get()
        if missing or filled:
            log.info('%r %r %r', '-'.join(stream[:5]), missing, filled)


if __name__ == '__main__':
    _initdb()
    check_stream()

