from apis.bittrex_api import filter_non_existing_markets
from apis.coinmarketcap_api import get_top_altcoins
from daos.bittrex_dao import BittrexDAO


def create_marketset(database_uri, collection_name, min_market_cap=None, limit=None):
    markets = _get_markets(min_market_cap=min_market_cap, limit=limit)
    with BittrexDAO(database_uri) as bittrex_dao:
        bittrex_dao.save_market_names(collection_name, markets)


def _get_markets(min_market_cap=None, limit=None):
    altcoins = get_top_altcoins(min_market_cap=min_market_cap, limit=limit)
    markets = ['BTC-' + coin['symbol'] for coin in altcoins]
    existing_markets = filter_non_existing_markets(markets)
    print("Marketset gathered successfully, {} markets gathered".format(len(existing_markets)))
    print("Non existing markets: ({})".format(len(markets) - len(existing_markets)))
    return existing_markets

