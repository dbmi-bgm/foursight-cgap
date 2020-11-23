from datetime import datetime, timedelta
from dateutil import tz


def parse_datetime_to_utc(time_str, manual_format=None):
    """
    Attempt to parse the string time_str with the given string format.
    If no format is given, attempt to automatically parse the given string
    that may or may not contain timezone information.
    Returns a datetime object of the string in UTC
    or None if the parsing was unsuccessful.
    """
    if manual_format and isinstance(manual_format, str):
        timeobj = datetime.strptime(time_str, manual_format)
    else:  # automatic parsing
        if len(time_str) > 26 and time_str[26] in ['+', '-']:
            try:
                timeobj = datetime.strptime(time_str[:26],'%Y-%m-%dT%H:%M:%S.%f')
            except ValueError:
                return None
            if time_str[26]=='+':
                timeobj -= timedelta(hours=int(time_str[27:29]), minutes=int(time_str[30:]))
            elif time_str[26]=='-':
                timeobj += timedelta(hours=int(time_str[27:29]), minutes=int(time_str[30:]))
        elif len(time_str) == 26 and '+' not in time_str[-6:] and '-' not in time_str[-6:]:
            # nothing known about tz, just parse it without tz in this cause
            try:
                timeobj = datetime.strptime(time_str[0:26],'%Y-%m-%dT%H:%M:%S.%f')
            except ValueError:
                return None
        else:
            # last try: attempt without milliseconds
            try:
                timeobj = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                return None
    return timeobj.replace(tzinfo=tz.tzutc())


def cat_indices(client):
    """ Wrapper function for the ES API _cat/indices so that the result returned is comprehensible.

        :param client: es client to use
        :returns: 2-tuple lists of header, rows
    """
    if not client:
        return [], []
    indices = client.cat.indices(v=True).split('\n')
    split_indices = [ind.split() for ind in indices]
    headers = split_indices.pop(0)  # first row is header
    return headers, split_indices
