import re
import itertools
from concurrent.futures import ThreadPoolExecutor
from retry import retry
from pymongo.errors import BulkWriteError, AutoReconnect

from apis.bittrex_api_wrapper import BittrexAPI


def update_market_data(markets, intervals, database):
    market_interval_pairs = itertools.product(markets, intervals)
    pool_args = [(market, interval, database) for market, interval in market_interval_pairs]
    print('{} seperate datasets to update'.format(len(pool_args)))
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda args: _get_and_save_ticks(*args), pool_args)


# Method is not separated to first get and then save because we want to make this 2 operations at the same time so
# RAM memory is not overloaded with holding all of the financial data before saving it
def _get_and_save_ticks(market_name, interval, database):
    ticks = BittrexAPI.get_ticks(market_name, interval)
    # non-alphanumeric characters are split for MongoDB to make later querying easier
    collection_name = interval + re.sub(r'\W+', '', market_name)
    collection = database[collection_name]
    _save_ticks_to_database(ticks, collection)
    print('{} for {} interval updated'.format(market_name, interval))


@retry(AutoReconnect, tries=5, delay=1, backoff=2)
def _save_ticks_to_database(ticks, collection):
    # create index to guarantee timespans uniqueness in case when the collection doesn't exist yet
    collection.create_index('T', unique=True)
    try:
        collection.insert_many(ticks, ordered=False)
    except BulkWriteError:
        # if there is even one duplicate in inserted data database throws BulkWriteError,
        # just ignore it, all non-duplicates were inserted successfully
        pass


