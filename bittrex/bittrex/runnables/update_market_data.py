import os
from bittrex.datagatherers.market_data_updater import update_market_data

# Specify properties for updating markets
INTERVALS = ['oneMin', 'fiveMin']  # Time intervals for which to gather data (look at Bittrex API for more information)
# Name of a collection containing names of the markets for which to gather data
MARKETSSET_COLLECTION_NAME = 'allMarkets'


DATABASE_URI_ENV = 'BITTREX_DATA_DB_URI'
if DATABASE_URI_ENV not in os.environ:
    raise EnvironmentError('Database URI is not set under {}, '
                           'please set it before running the script again'.format(DATABASE_URI_ENV))

if __name__ == '__main__':
    database_uri = os.environ[DATABASE_URI_ENV]
    update_market_data(INTERVALS, database_uri, MARKETSSET_COLLECTION_NAME)
    print('Data updated successfully')
