import re
import threading
from retry import retry
from pymongo.errors import BulkWriteError, AutoReconnect

from utils.bittrex_api_wrapper import BittrexAPI


def update_market_data(market_names, database):
    gatherers = []
    for market in market_names:
        market_updater = _MarketDataUpdater(market, database)
        market_updater.start()
        gatherers.append(market_updater)

    for gath in gatherers:
        gath.join()


class _MarketDataUpdater(threading.Thread):
    def __init__(self, market_name, database):
        super(_MarketDataUpdater, self).__init__()
        self.market_name = market_name
        self.database = database

    def run(self):
        onemin_ticks = BittrexAPI.get_ticks(self.market_name, 'oneMin')
        # non-alphanumeric characters are split for MongoDB to make later querying easier
        self._save_ticks_to_database(onemin_ticks, 'oneMin' + re.sub(r'\W+', '', self.market_name))
        fivemin_ticks = BittrexAPI.get_ticks(self.market_name, 'fiveMin')
        # non-alphanumeric characters are split for MongoDB to make later querying easier
        self._save_ticks_to_database(fivemin_ticks, 'fiveMin' + re.sub(r'\W+', '', self.market_name))

    @retry(AutoReconnect, tries=5, delay=1, backoff=2)
    def _save_ticks_to_database(self, ticks, collection_name):
        market_collection = self.database[collection_name]
        # create index to guarantee timespans uniqueness in case when the collection doesn't exist yet
        market_collection.create_index('T', unique=True)
        try:
            market_collection.insert_many(ticks, ordered=False)
        except BulkWriteError as bwe:
            # if there is even one duplicate in inserted data database throws BulkWriteError,
            # just ignore it, all non-duplicates were inserted successfully
            pass
