__author__ = 'sgfoote'

import re
from datetime import datetime

UNIX_EPOCH_DATETIME = datetime(1970, 1, 1)
MILLIS_PER_DAY = 24 * 60 * 60 * 1000
NTP_UNIX_DELTA_SECONDS = 2208988800
EXTENDED_ISO8601_DATE_REGEX = re.compile("""\\d{4}[_-]?(0[1-9]|1[0-2])[-_]?(0[1-9]|[12][0-9]|
                        3[01])""")


def java_time_to_iso8601(java_time):
    """
    Convert an integer timestamp representing milliseconds since the UNIX epoch to an ISO8601 string representation of
    that time. E.g. an input of 1494524856000 results in an output of "2017-05-11T17:47:36".
    :param java_time: an integer representing milliseconds since the UNIX epoch
    :return: a string representing the input time in ISO8601 format
    """
    return datetime.utcfromtimestamp(java_time / 1000).isoformat()


def java_time_to_iso8601_basic_date(java_time):
    """
    Convert an integer timestamp representing milliseconds since the UNIX epoch to a date in the format yyyy-mm-dd.
    Time precision beyond day of the year is ignored.
    :param java_time: an integer representing milliseconds since the UNIX epoch
    :return: a string representing the date associated with the input time in the format yyyymmdd
    """
    return datetime.utcfromtimestamp(java_time/1000.0).date().strftime('%Y%m%d')


def ntp_to_java_time(ntp_time):
    """
    Convert a double timestamp representing Network Time Protocol (NTP) time to a long timestamp representing
    milliseconds since the UNIX epoch. E.g. an input of 3606226616.774 results in an output of 1397237817000L.
    :param ntp_time: a double timestamp representing NTP time
    :return: a long representing the input time as seconds since the UNIX epoch
    """
    return long(round(ntp_time - NTP_UNIX_DELTA_SECONDS) * 1000)


def parse_basic_iso8601_date(parse_string):
    """

    :param parse_string:
    :return:
    """
    match = re.search(EXTENDED_ISO8601_DATE_REGEX, parse_string)
    if not match:
        return None
    # normalize delimeters to ""
    normalized_date = match.group().replace("-", "").replace("-", "")
    return datetime.strptime(normalized_date, "%Y%m%d")


def java_time_from_basic_iso8601_date(date):
    """

    :param date_string:
    :return:
    """
    return (date - UNIX_EPOCH_DATETIME).total_seconds() * 1000