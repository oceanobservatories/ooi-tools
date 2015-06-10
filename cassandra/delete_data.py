"""
Delete a specific reference designator from cassandra

Usage:
    delete_stream.py refdes <refdes>
    delete_stream.py subsite <subsite>

"""

from cassandra.cluster import Cluster


cluster = Cluster(control_connection_timeout=60)
session = cluster.connect('ooi')

lookup_stream_method = session.prepare('select stream, method, count from stream_metadata where subsite=? and node=? and sensor=?')
del_metadata = session.prepare('delete from stream_metadata where subsite=? and node=? and sensor=? and method=? and stream=?')
del_metadata_hourly = session.prepare('delete from stream_metadata_hourly where subsite=? and node=? and sensor=? and method=? and stream=?')

def delete_metadata(subsite, node, sensor, method, stream):
    session.execute(del_metadata, (subsite, node, sensor, method, stream))
    session.execute(del_metadata_hourly, (subsite, node, sensor, method, stream))


def delete_stream(subsite, node, sensor, stream):
    prepared = session.prepare('delete from %s where subsite=? and node=? and sensor=?' % stream)
    session.execute(prepared, (subsite, node, sensor))


def parse_refdes(refdes):
    return refdes.split('-', 2)


def find_stream_methods(subsite, node, sensor):
    return list(session.execute(lookup_stream_method, (subsite, node, sensor)))


def find_subsite_desigs(subsite):
    return [row for row in session.execute('select subsite, node, sensor from stream_metadata')
            if row.subsite == subsite]


def delete_refdes(subsite, node, sensor):
    rows = find_stream_methods(subsite, node, sensor)
    for row in rows:
        print 'Deleting:', row
        delete_metadata(subsite, node, sensor, row.method, row.stream)
        delete_stream(subsite, node, sensor, row.stream)


def main():
    import docopt
    options = docopt.docopt(__doc__)
    if options['subsite']:
        for row in find_subsite_desigs(options['<subsite>']):
            delete_refdes(row.subsite, row.node, row.sensor)

    elif options['refdes']:
        subsite, node, sensor = parse_refdes(options['<refdes>'])
        delete_refdes(subsite, node, sensor)


if __name__ == '__main__':
    main()
