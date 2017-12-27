import os
from pymongo import MongoClient

from datagatherers.market_data_updater import update_market_data


MARKETSSET_COLLECTION_NAME = "allMarkets"
DATABASE_URI_ENV = 'BITTREX_DATA_DB_URI'
if DATABASE_URI_ENV not in os.environ:
    raise EnvironmentError('Database URI is not set under {}, '
                           'please set it before running the script again'.format(DATABASE_URI_ENV))

database_uri = os.environ[DATABASE_URI_ENV]
db_name = database_uri.rsplit('/', 1)[-1]
db_client = MongoClient(database_uri, maxPoolSize=None)
database = db_client[db_name]

marketset_collection = database[MARKETSSET_COLLECTION_NAME]
marketset = marketset_collection.find()
marketset = list(map(lambda market: market['market_name'], marketset))
update_market_data(marketset, database)
print('Data updated successfully')
