import json
from concurrent.futures import ThreadPoolExecutor
from dateutil.parser import parse
from enum import Enum
from retry import retry
from urllib import request as url_request
from urllib.error import URLError

OPEN_LABEL = 'O'
HIGH_LABEL = 'H'
LOW_LABEL = 'L'
CLOSE_LABEL = 'C'
VOLUME_LABEL = 'V'
TIMESPAN_LABEL = 'T'
BASE_VOLUME_LABEL = 'BV'

MARKET_TICKS_ENDPOINT = 'https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName={}&tickInterval={}'
MARKET_SUMMARY_ENDPOINT = 'https://bittrex.com/api/v1.1/public/getmarketsummary?market={}'
NON_EXISTING_MARKET_MESSAGE = 'INVALID_MARKET'


class Interval(Enum):
    oneMin = 1
    fiveMin = 5
    thirtyMin = 30
    hour = 60
    day = 1440


@retry(URLError, tries=5, delay=1, backoff=2)
def get_ticks(market, interval):
    request_url = MARKET_TICKS_ENDPOINT.format(market, interval)
    with url_request.urlopen(request_url) as connection:
        response = connection.read()
        response = json.loads(response.decode('utf-8'))

    if response['success']:
        ticks = response['result']
        return _ticks_timespan_to_date_object(ticks)

    raise ConnectionError('Bittrex API returned failed response for the market {} with a message {}'
                          .format(market, response['message']))


def _ticks_timespan_to_date_object(ticks):
    for t in ticks:
        t[TIMESPAN_LABEL] = parse(t[TIMESPAN_LABEL])

    return ticks


def filter_non_existing_markets(markets):
    """Takes a list of market names and returns all markets from the list that exist one the Bittrex exchange"""
    with ThreadPoolExecutor(max_workers=10) as executor:
        markets_existance = zip(markets, executor.map(does_exist, markets))

    return [market for market, exists in markets_existance if exists]


@retry(URLError, tries=5, delay=1, backoff=2)
def does_exist(market):
    """Takes name of a crypto market, returns True if it exists on the Bittrex exchange and False otherwise"""
    request_url = MARKET_SUMMARY_ENDPOINT.format(market)
    with url_request.urlopen(request_url) as connection:
        response = connection.read()
        response = json.loads(response.decode('utf-8'))

    if response['success']:
        return True

    if response['message'] == NON_EXISTING_MARKET_MESSAGE:
        return False

    raise ConnectionError('There is a problem with request url, either on our or on the bittrex side,'
                          'try to run {} url in a browser to investigate'.format(request_url))
