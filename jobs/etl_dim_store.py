import pandas as pd
import sqlalchemy as db
import configparser
import logging
from logging.config import fileConfig

# Configs
config = configparser.ConfigParser()
config.read('conf/.env')

fileConfig('conf/logging_config.ini')
logger = logging.getLogger()

# Database connection URI
db_engine = db.create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format(
        config['database']['user'],
        config['database']['password'],
        config['database']['host'],
        config['database']['port'],
        config['database']['db']
    )
)

# Data warehouse connection URI
dw_engine = db.create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format(
        config['data-warehouse']['user'],
        config['data-warehouse']['password'],
        config['data-warehouse']['host'],
        config['data-warehouse']['port'],
        config['data-warehouse']['db']
    )
)

############################################
# FUNCTIONS
############################################


def get_dimStore_last_id(database):
    """ Function to get last_id in dim_store"""
    query = "SELECT max(store_id) AS last_id FROM dim_store"
    tdf = pd.to_sql(query, database)
    return tdf.iloc[0]['last_id']


def extract_table_store(last_id, database):
    """ Function to extract table `store` """
    if last_id == None:
        last_id = -1

    query = "SELECT * FROM store WHERE store_id > {} LIMIT 100000".format(
        last_id)
    return pd.read_sql(query, database)


def lookup_table_address(store_df, database):
    """ Function to lookup table `address`"""
    unique_ids = list(store_df.address_id.unique())
    unique_ids = list(filter(None, unique_ids))

    query = "SELECT * FROM address WHERE address_in IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, database)


def lookup_table_city(address_df, database):
    """ Function to lookup table `city` """
    unique_ids = list(address_df.city_id.unique())
    unique_ids = list(filter(None, unique_ids))

    query = "SELECT * FROM city WHERE city_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, database)


def lookup_table_country(city_df, databse):
    """ Function to lookup table `country` """
    unique_ids = list(city_df.country_id.unique())
    unique_ids = list(filter(None, unique_ids))

    query = "SELECT * FROM country WHERE country_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, databse)


def lookup_table_staff(store_df, database):
    """ Function to lookup table `staff` """
    unique_ids = list(store_df.manager_staff_id.unique())
    unique_ids = list(filter(None, unique_ids))

    query = "SELECT * FROM staff WHERE staff_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, database)


def join_store_address(store_df, address_df):
    """ Transformation: join table `store` and `address` """
    store_df = pd.merge(store_df, address_df,
                        left_on='address_id', right_on='address_id', how='left')
    store_df = store_df[['store_id', 'manager_staff_id', 'last_update_x',
                         'address', 'address2', 'district', 'city_id', 'postal_code']]
    return store_df


def join_store_city(store_df, city_df):
    """ Transformation: join table `store` and `city` """
    store_df = pd.merge(store_df, city_df, left_on='city_id',
                        right_on='city_id', how='left')
    store_df = store_df[['store_id', 'manager_staff_id', 'last_update_x',
                         'address', 'address2', 'district', 'city', 'country_id', 'postal_code']]
    store_df = store_df.rename({'last_update_x': 'last_update'}, axis=1)
    return store_df


def join_store_country(store_df, country_df):
    """ Transformation: join table `store` and `country` """
    store_df = pd.merge(store_df, country_df,
                        left_on='country_id', right_on='country_id', how='left')
    store_df = store_df[['store_id', 'manager_staff_id', 'last_update_x',
                         'address', 'address2', 'district', 'city', 'country', 'postal_code']]
    store_df = store_df.rename({'last_update_x': 'last_update'}, axis=1)
    return store_df


def join_store_manager_staff(store_df, staff_df):
    """ Transformation: join table `store` and `manager_staff` """
    store_df = pd.merge(
        store_df, staff_df, left_on='manager_staff_id', right_on='staff_id', how='left')
    store_df = store_df[['store_id_x', 'address', 'address2', 'district',
                         'city', 'country', 'postal_code', 'first_name', 'last_name']]
    store_df = store_df.rename(
        {'store_id_x': 'store_id', 'first_name': 'manager_first_name', 'last_name': 'manager_last_name'}, axis=1)
    return store_df


def validate(source_df, destination_df):
    """ Function to validate transformation result """
    source_row_count = source_df.shape[0]
    destination_row_count = destination_df.shape[0]

    if(source_row_count == destination_row_count):
        return destination_df
    else:
        raise ValueError(
            'Transformation result is not valid: row count is not equal')


def load_dim_store(destination_df):
    destination_df.to_sql('dim_store', dw_engine,
                          if_exists='append', index=False)

############################################
# EXTRACT
############################################
