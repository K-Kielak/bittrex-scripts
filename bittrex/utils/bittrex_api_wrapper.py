from threading import Thread
from retry import retry
from urllib import request as url_request
from urllib.error import URLError
import json


class BittrexAPI:
    MARKET_TICKS_ENDPOINT = 'https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName={}&tickInterval={}'
    MARKET_SUMMARY_ENDPOINT = 'https://bittrex.com/api/v1.1/public/getmarketsummary?market={}'
    NON_EXISTING_MARKET_MESSAGE = 'INVALID_MARKET'

    @staticmethod
    @retry(URLError, tries=5, delay=1, backoff=2)
    def get_ticks(market, interval):
        request_url = BittrexAPI.MARKET_TICKS_ENDPOINT.format(market, interval)
        with url_request.urlopen(request_url) as connection:
            response = connection.read()
            response = json.loads(response.decode('utf-8'))

        if response['success']:
            return response['result']

        raise ConnectionError('Bittrex API returned failed response for the market {} with a message {}'
                              .format(market, response['message']))

    @staticmethod
    def filter_non_existing_markets(markets):
        """Takes a list of market names and returns all markets from the list that exist one the Bittrex exchange"""
        checker_threads = []
        existing_markets = []
        non_existing_markets = []
        for market in markets:
            checker = Thread(target=BittrexAPI._classify_markets, args=(market, existing_markets,non_existing_markets))
            checker.start()
            checker_threads.append(checker)

        for checker in checker_threads:
            checker.join()

        return existing_markets, non_existing_markets

    @staticmethod
    @retry(URLError, tries=5, delay=1, backoff=2)
    def exists(market):
        """Takes name of a crypto market, returns True if it exists on the Bittrex exchange and False otherwise"""
        request_url = BittrexAPI.MARKET_SUMMARY_ENDPOINT.format(market)
        with url_request.urlopen(request_url) as connection:
            response = connection.read()
            response = json.loads(response.decode('utf-8'))

        if response['success']:
            return True

        if response['message'] == BittrexAPI.NON_EXISTING_MARKET_MESSAGE:
            return False

        raise ConnectionError('There is a problem with request url, either on our or on the bittrex side,'
                              'try to run {} url in a browser to investigate'.format(request_url))

    @staticmethod
    def _classify_markets(market, existing_markets, non_existing_markets):
        if BittrexAPI.exists(market):
            existing_markets.append(market)
        else:
            non_existing_markets.append(market)
