from pymongo.errors import BulkWriteError

from apis.bittrex_api_wrapper import BittrexAPI
from apis.coinmarketcap_api_wrapper import CoinmarketcapAPI


def create_marketset(db_collection, min_market_cap=None, limit=None):
    """Creates set of names for altcoin markets that meet given requirements and saves it in a given MongoDB database"""
    markets = _get_markets(min_market_cap=min_market_cap, limit=limit)
    _save_markets(db_collection, markets)


def _get_markets(min_market_cap=None, limit=None):
    altcoins = CoinmarketcapAPI.get_top_altcoins(min_market_cap=min_market_cap, limit=limit)
    markets = ['BTC-' + coin['symbol'] for coin in altcoins]
    existing_markets = BittrexAPI.filter_non_existing_markets(markets)
    print("Marketset gathered successfully, {} markets gathered".format(len(existing_markets)))
    print("Non existing markets: ({})".format(len(markets) - len(existing_markets)))

    return existing_markets


def _save_markets(db_collection, markets):
    # map list of names to the appropiate json format
    altcoin_markets = [{'market_name': market_name} for market_name in markets]
    # create index to guarantee datagatherers uniqueness in case when the collection doesn't exist yet
    db_collection.create_index('market_name', unique=True)
    try:
        db_collection.insert_many(altcoin_markets, ordered=False)
    except BulkWriteError:
        # if there is even one duplicate in inserted data database throws BulkWriteError,
        # just ignore it, all non-duplicates were inserted successfully
        pass

