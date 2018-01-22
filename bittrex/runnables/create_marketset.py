# Script that chooses markets of altcoins and saves them to the database.
# Run it if you want to start gathering data about some additional altcoin datagatherers.

# If you want to delete or stop gathering data for a specific altcoin market (and you are 200% sure you want to do it)
# you need to do it manually from the database. This is due to the fact that continous data is critical at the moment,
# and we don't want to stop gathering it, or even worse, delete it by accident.

import os
from bittrex.datagatherers.marketset_creator import create_marketset

# Specify properties for creating marketset
LIMIT = None  # limits the result to the top <limit> altcoins by market cap
MIN_MARKET_CAP = 20000000  # specifies the minimal market_cap for chosen chosen altcoin
# specifies the name of the collection where market names of a chosen altcoins will be saved in the database,
# 'allMarkets' is default name where for markets for which data is gathered, use different name if you want to create
# different marketset or marketsubset
COLLECTION_NAME = 'allMarkets'


DATABASE_URI_ENV = 'BITTREX_DATA_DB_URI'
if DATABASE_URI_ENV not in os.environ:
    raise EnvironmentError('Database URI is not set under {}, '
                           'please set it before running the script again'.format(DATABASE_URI_ENV))

if __name__ == '__main__':
    database_uri = os.environ[DATABASE_URI_ENV]
    create_marketset(database_uri, COLLECTION_NAME, min_market_cap=MIN_MARKET_CAP, limit=LIMIT)
