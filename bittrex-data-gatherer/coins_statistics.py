from urllib import request as url_request
import json


class CoinsStatistics:
    COINS_STATISTICS_ENDPOINT = 'https://api.coinmarketcap.com/v1/ticker?limit=0'

    # gets data from API on __init__, should raise exception when something wrong happens
    def __init__(self):
        with url_request.urlopen(CoinsStatistics.COINS_STATISTICS_ENDPOINT) as connection:
            response = connection.read()
            self.coins = json.loads(response.decode('utf-8'))

    def get_top_altcoins(self, min_market_cap=None, limit=None):
        # exclude bitcoin because it is not an altcoin
        altcoins = (filter(lambda coin: coin['symbol'] != 'BTC', self.coins))
        if min_market_cap is not None:
            altcoins = filter(lambda coin: float(coin['market_cap_usd'] or 0) > min_market_cap, altcoins)

        if limit is not None:
            altcoins = sorted(altcoins, key=lambda coin: float(coin['market_cap_usd'] or 0))[:90]

        return list(altcoins)
