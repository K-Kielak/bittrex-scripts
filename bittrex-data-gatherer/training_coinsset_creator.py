# Script that chooses altcoins for which markets_data_gatherer methods gathers data that will/is used for
# machine learning. Run it if you want to start gathering data about some additional altcoins.

# If you want to delete or stop gathering data for a specific altcoin (and you are 200% sure you want to do it)
# you need to do it manually from the database. This is due to the fact that continous data is critical at the moment,
# and we don't want to stop gathering it, or even worse, delete it by accident.

import os
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from coins_statistics import CoinsStatistics
from bittrex_api_wrapper import BittrexAPI

# Specify properties for the training coinsset
LIMIT = None  # limits the result to the top <limit> altcoins by market cap
MIN_MARKET_CAP = 20000000  # specifies the minimal market_cap for chosen chosen altcoin
# specifies the name of the collection where market names of a chosen altcoins will be saved in the database
COLLECTION_NAME = 'markets'


DATABASE_URI_ENV = 'BITTREX_DATA_DB_URI'
if DATABASE_URI_ENV not in os.environ:
    raise EnvironmentError('Database URI is not set under {}, '
                           'please set it before running the script again'.format(DATABASE_URI_ENV))

coinsStatistics = CoinsStatistics()
altcoins = coinsStatistics.get_top_altcoins(min_market_cap=MIN_MARKET_CAP, limit=LIMIT)
altcoin_markets = map(lambda coin: 'BTC-' + coin['symbol'], altcoins)
altcoin_markets, non_existing_markets = BittrexAPI.filter_non_existing_markets(altcoin_markets)
# map list of names to the appropiate json format
altcoin_markets = list(map(lambda market_name: {'market_name': market_name}, altcoin_markets))

database_uri = os.environ[DATABASE_URI_ENV]
# database name is always the string after the last slash ('/')
db_name = database_uri.rsplit('/', 1)[-1]
db_client = MongoClient(database_uri)
markets_collection = db_client[db_name][COLLECTION_NAME]

# create index to guarantee markets uniqueness in case when the collection doesn't exist yet
markets_collection.create_index('market_name', unique=True)
try:
    markets_collection.insert_many(altcoin_markets, ordered=False)
except BulkWriteError as bwe:
    # if there is even one duplicate in inserted data database throws BulkWriteError,
    # just ignore it, all non-duplicates were inserted successfully
    pass

print("Coinsset created/updated successfully, {} markets gathered".format(len(altcoin_markets)))
print("Non existing markets: ({})".format(len(non_existing_markets)))
for market in non_existing_markets:
    print(market)
