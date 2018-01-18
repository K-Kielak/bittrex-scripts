import json
from retry import retry
from urllib import request as url_request
from urllib.error import URLError


class CoinmarketcapAPI(object):
    COINS_STATISTICS_ENDPOINT = 'https://api.coinmarketcap.com/v1/ticker?limit=0'

    @staticmethod
    @retry(URLError, tries=5, delay=1, backoff=2)
    def get_all_coins():
        """Returns all coins from the Coinmarketcap site"""
        with url_request.urlopen(CoinmarketcapAPI.COINS_STATISTICS_ENDPOINT) as connection:
            response = connection.read()
            coins = json.loads(response.decode('utf-8'))

        return coins

    @staticmethod
    def get_top_altcoins(min_market_cap=None, limit=None):
        coins = CoinmarketcapAPI.get_all_coins()
        # exclude bitcoin because it is not an altcoin
        altcoins = (filter(lambda coin: coin['symbol'] != 'BTC', coins))
        if min_market_cap is not None:
            altcoins = filter(lambda coin: float(coin['market_cap_usd'] or 0) > min_market_cap, altcoins)

        if limit is not None:
            altcoins = sorted(altcoins, key=lambda coin: float(coin['market_cap_usd'] or 0))[:90]

        return list(altcoins)
