# Script that chooses markets of altcoins for which data will be gathered that will/is used for
# machine learning. Run it if you want to start gathering data about some additional altcoin markets.

# If you want to delete or stop gathering data for a specific altcoin market (and you are 200% sure you want to do it)
# you need to do it manually from the database. This is due to the fact that continous data is critical at the moment,
# and we don't want to stop gathering it, or even worse, delete it by accident.

import os
from pymongo import MongoClient

from marketset.marketset_creator import create_marketset

# Specify properties for the training coinsset
LIMIT = None  # limits the result to the top <limit> altcoins by market cap
MIN_MARKET_CAP = 20000000  # specifies the minimal market_cap for chosen chosen altcoin
# specifies the name of the collection where market names of a chosen altcoins will be saved in the database
COLLECTION_NAME = 'markets'


DATABASE_URI_ENV = 'BITTREX_DATA_DB_URI'
if DATABASE_URI_ENV not in os.environ:
    raise EnvironmentError('Database URI is not set under {}, '
                           'please set it before running the script again'.format(DATABASE_URI_ENV))

database_uri = os.environ[DATABASE_URI_ENV]
# database name is always the string after the last slash ('/')
db_name = database_uri.rsplit('/', 1)[-1]
db_client = MongoClient(database_uri)
markets_collection = db_client[db_name][COLLECTION_NAME]
create_marketset(markets_collection, min_market_cap=MIN_MARKET_CAP, limit=LIMIT)
