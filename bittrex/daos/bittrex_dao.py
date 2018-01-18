from pymongo import MongoClient
from pymongo.errors import AutoReconnect
from pymongo.errors import BulkWriteError
from retry import retry


class BittrexDAO(object):
    def __init__(self, database_uri):
        db_name = database_uri.rsplit('/', 1)[-1]
        self.db_client = MongoClient(database_uri)
        self.database = self.db_client[db_name]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.db_client.close()

    @retry(AutoReconnect, tries=5, delay=1, backoff=2)
    def save_market_names(self, marketset_collection_name, markets):
        collection = self.database[marketset_collection_name]
        # map list of names to the appropiate json format
        markets = [{'market_name': market_name} for market_name in markets]
        # create index to guarantee datagatherers uniqueness in case when the collection doesn't exist yet
        collection.create_index('market_name', unique=True)
        try:
            collection.insert_many(markets, ordered=False)
        except BulkWriteError:
            # if there is even one duplicate in inserted data database throws BulkWriteError,
            # just ignore it, all non-duplicates were inserted successfully
            pass

    @retry(AutoReconnect, tries=5, delay=1, backoff=2)
    def get_market_names(self, marketset_collection_name):
        marketset_collection = self.database[marketset_collection_name]
        marketset = marketset_collection.find()
        return [market['market_name'] for market in marketset]

    @retry(AutoReconnect, tries=5, delay=1, backoff=2)
    def save_ticks(self, ticks, ticks_type):
        """
        Saves ticks to database
        :param ticks: market/interval ticks
        :param ticks_type: ticks type, should be in format:
                <bittrex-interval><base_coin_symbol><quote_coin_symbol>; i.e: oneMinBTCOMG
        """
        collection = self.database[ticks_type]
        # create index to guarantee timespans uniqueness in case when the collection doesn't exist yet
        collection.create_index('T', unique=True)
        try:
            collection.insert_many(ticks, ordered=False)
        except BulkWriteError:
            # if there is even one duplicate in inserted data database throws BulkWriteError,
            # just ignore it, all non-duplicates were inserted successfully
            pass
