import pymongo
from apis.bittrex_api import TIMESPAN_LABEL  # processed data uses the same labels for ticks as bittrex data
from pymongo import MongoClient
from pymongo.errors import AutoReconnect
from pymongo.errors import BulkWriteError
from retry import retry

TICKS_LABEL = 'Ticks'


class ProcessedDataDAO(object):
    def __init__(self, database_uri):
        db_name = database_uri.rsplit('/', 1)[-1]
        self.db_client = MongoClient(database_uri)
        self.database = self.db_client[db_name]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.db_client.close()

    @retry(AutoReconnect, tries=5, delay=1, backoff=2)
    def save_states(self, states, ticks_type):
        """
        Saves states to database
        :param states: ticks preprocessed to state format
        :param ticks_type: ticks type, should be in format:
                <bittrex-interval><base_coin_symbol><quote_coin_symbol>; i.e: oneMinBTCOMG
        """
        collection = self.database[ticks_type]
        # create index to guarantee timespans uniqueness in case when the collection doesn't exist yet
        collection.create_index(TIMESPAN_LABEL, unique=True)
        try:
            collection.insert_many(states, ordered=False)
        except BulkWriteError:
            # if there is even one duplicate in inserted data database throws BulkWriteError,
            # just ignore it, all non-duplicates were inserted successfully
            pass

    @retry(AutoReconnect, tries=5, delay=1, backoff=2)
    def get_latest_state_timespan(self, ticks_type):
        """
        :param ticks_type: ticks type, should be in format:
            <bittrex-interval><base_coin_symbol><quote_coin_symbol>; i.e: oneMinBTCOMG
        :return: timespan date object of the most recent state in the database by timespan
        """
        db_response = self.database[ticks_type]\
            .find({}, {TIMESPAN_LABEL: True, '_id': False})\
            .sort(TIMESPAN_LABEL, pymongo.DESCENDING)\
            .limit(1)

        return list(db_response)[0][TIMESPAN_LABEL]
