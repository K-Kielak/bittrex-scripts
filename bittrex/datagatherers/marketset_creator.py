from pymongo.errors import BulkWriteError

from utils.bittrex_api_wrapper import BittrexAPI
from utils.coins_statistics import CoinsStatistics


def create_marketset(db_collection, min_market_cap=None, limit=None):
    """Creates set of names for altcoin markets that meet given requirements and saves it in a given MongoDB database"""
    markets = _get_markets(min_market_cap=min_market_cap, limit=limit)
    _save_markets(db_collection, markets)


def _get_markets(min_market_cap=None, limit=None):
    coins_statistics = CoinsStatistics()
    altcoins = coins_statistics.get_top_altcoins(min_market_cap=min_market_cap, limit=limit)
    altcoin_markets = map(lambda coin: 'BTC-' + coin['symbol'], altcoins)
    altcoin_markets, non_existing_markets = BittrexAPI.filter_non_existing_markets(altcoin_markets)

    print("Marketset gathered successfully, {} datagatherers gathered".format(len(altcoin_markets)))
    print("Non existing datagatherers: ({})".format(len(non_existing_markets)))
    for market in non_existing_markets:
        print(market)

    return altcoin_markets


def _save_markets(db_collection, markets):
    # map list of names to the appropiate json format
    altcoin_markets = list(map(lambda market_name: {'market_name': market_name}, markets))
    # create index to guarantee datagatherers uniqueness in case when the collection doesn't exist yet
    db_collection.create_index('market_name', unique=True)
    try:
        db_collection.insert_many(altcoin_markets, ordered=False)
    except BulkWriteError as bwe:
        # if there is even one duplicate in inserted data database throws BulkWriteError,
        # just ignore it, all non-duplicates were inserted successfully
        pass

