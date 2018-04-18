import re
import itertools
from bittrex.apis.bittrex_api import get_ticks
from bittrex.daos.bittrex_dao import BittrexDAO
from concurrent.futures import ThreadPoolExecutor


def update_market_data(intervals, database_uri, collection_name):
    with BittrexDAO(database_uri) as bittrex_dao:
        markets = bittrex_dao.get_market_names(collection_name)
        market_interval_pairs = itertools.product(markets, intervals)
        pool_args = [(market, interval, bittrex_dao) for market, interval in market_interval_pairs]
        print('{} seperate datasets to update'.format(len(pool_args)))
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = executor.map(lambda args: _get_and_save_ticks(*args), pool_args)
            # check for exceptions
            for _ in futures:
                pass


# Method is not separated to first get and then save because we want to make this 2 operations at the same time so
# RAM memory is not overloaded with holding all of the financial data before saving it
def _get_and_save_ticks(market_name, interval, dao):
    ticks = get_ticks(market_name, interval)
    if not ticks:
        return

    # non-alphanumeric characters are split for MongoDB to make later querying easier
    ticks_type = interval + re.sub(r'\W+', '', market_name)
    dao.save_ticks(ticks, ticks_type)
    print('{} for {} interval updated'.format(market_name, interval))
