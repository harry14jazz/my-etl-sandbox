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


def get_factSales_last_id(database):
    """ Function to get last sales_key from fact table `fact_sales` """
    query = "SELECT max(sales_key) AS last_id FROM fact_sales"
    tdf = pd.read_sql(query, database)
    return tdf.iloc[0]['last_id']


def extract_table_payment(last_id, database):
    """ Function to extract table `payment` """
    if last_id == None:
        last_id = -1

    query = "SELECT * FROM payment WHERE payment_id > {} LIMIT 100000".format(
        last_id)
    return pd.read_sql(query, database)


def lookup_dim_customer(payment_df, database):
    """ Function to lookup table `dim_customer` """
    unique_ids = list(payment_df.customer_id.unique())
    unique_ids = list(filter(None, unique_ids))

    query = "SELECT * FROM dim_customer WHERE customer_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, database)


def lookup_table_rental(payment_df, database):
    """ Function to lookup table `rental` """
    payment_df = payment_df.dropna(how='any', subset=['rental_id'])
    unique_ids = list(payment_df.rental_id.unique())

    query = "SELECT * FROM rental WHERE rental_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, database)


def lookup_table_inventory(rental_df, database):
    """ Function to lookup table `Inventory` """
    rental_df = rental_df.dropna(how='any', subset=['inventory_id'])
    unique_ids = list(rental_df.inventory_id.unique())

    query = "SELECT * FROM inventory WHERE inventory_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, database)


def lookup_dim_movie(inventory_df, database):
    """ Function to lookup table `dim_movie` """
    inventory_df = inventory_df.dropna(how='any', subset=['film_id'])
    unique_ids = list(inventory_df.film_id.unique())

    query = "SELECT * FROM dim_movie WHERE film_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, database)


def lookup_dim_store(inventory_df, database):
    """ Function to lookup table `dim_store` """
    inventory_df = inventory_df.dropna(how='any', subset=['store_id'])
    unique_ids = list(inventory_df.store_id.unique())

    query = "SELECT * FROM dim_store WHERE store_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, database)


def join_payment_dim_customer(payment_df, dim_customer_df):
    """ Transform: join table payment and dim_customer """
    payment_df = pd.merge(payment_df, dim_customer_df,
                          left_on='customer_id', right_on='customer_id', how='left')
    payment_df = payment_df[['payment_id', 'customer_key', 'customer_id',
                             'rental_id', 'amount', 'payment_date', 'start_date', 'end_date']]
    payment_df = payment_df.rename({
        'start_date': 'customer_start_date',
        'end_date': 'customer_end_date'
    }, axis=1)
    # make sure we only join wiht customer record that's active at the time of transaction date
    payment_df = payment_df[(pd.to_datetime(payment_df.customer_start_date) <= payment_df.payment_date) & (
        (pd.to_datetime(payment_df.customer_end_date) >= payment_df.payment_date) | (payment_df.customer_end_date.isnull()))]

    return payment_df


def join_payment_rental(payment_df, rental_df):
    """ Transformation: join table `payment` and `rental` """
    payment_df = pd.merge(payment_df, rental_df,
                          left_on='rental_id', right_on='rental_id', how='left')
    payment_df = payment_df[['payment_id', 'customer_key',
                             'inventory_id', 'amount', 'payment_date', ]]
    return payment_df


def join_payment_inventory(payment_df, inventory_df):
    """ Transformation: join table `payment` and `inventory` """
    payment_df = pd.merge(payment_df, inventory_df,
                          left_on='inventory_id', right_on='inventory_id', how='left')
    payment_df = payment_df[['payment_id', 'customer_key',
                             'film_id', 'store_id', 'amount', 'payment_date', ]]
    return payment_df


def join_payment_dim_movie(payment_df, dim_movie_df):
    """ Transformation: join table `payment` and `dim_movie` """
    payment_df = pd.merge(payment_df, dim_movie_df,
                          left_on='film_id', right_on='film_id', how='left')
    payment_df = payment_df[['payment_id', 'customer_key',
                             'movie_key', 'store_id', 'amount', 'payment_date', ]]
    return payment_df


def join_payment_dim_store(payment_df, dim_store_df):
    """ Transformation: join table `payment` and `dim_store` """
    payment_df = pd.merge(payment_df, dim_store_df,
                          left_on='store_id', right_on='store_id', how='left')
    payment_df = payment_df[['payment_id', 'customer_key', 'movie_key',
                             'store_key', 'amount', 'payment_date', 'start_date', 'end_date']]
    payment_df = payment_df.rename(
        {'start_date': 'store_start_date', 'end_date': 'store_end_date'}, axis=1)
    # Make sure we only join with store record that is active at the time of transaction_date
    payment_df = payment_df[(pd.to_datetime(payment_df.store_start_date) <= payment_df.payment_date)
                            & ((pd.to_datetime(payment_df.store_end_date) >= payment_df.payment_date) | (payment_df.store_end_date.isnull()))
                            | (payment_df.store_key.isnull())]
    return payment_df


def add_date_key(payment_df):
    """ Add date_key smart key """
    payment_df['date_key'] = payment_df.payment_date.dt.strftime('%Y%m%d')
    return payment_df


def rename_remove_columns(payment_df):
    """ Rename and remove columns """
    payment_df = payment_df.rename({
        'payment_id': 'sales_key',
        'amount': 'sales_amount'
    }, axis=1)
    payment_df = payment_df[['sales_key', 'date_key',
                             'customer_key', 'movie_key', 'store_key', 'sales_amount']]
    return payment_df


def validate(source_df, destination_df):
    """ Function to validate transformation result """
    # Make sure row count is qual between source and destination
    source_row_count = source_df.shape[0]
    destination_row_count = destination_df.shape[0]

    if(source_row_count != destination_row_count):
        raise ValueError('Transformation result is not valid: row count is not equal (source={}; destination={})'.format(
            source_row_count, destination_row_count))

    # Make sure there is no null value in all dimension key
    if destination_df['customer_key'].hasnans:
        raise ValueError(
            'Transformation result is not valid: column customer_key has NaN value'
        )
    return destination_df


def load_dim_payment(destination_df):
    """ Load to data warehouse """
    destination_df.to_sql('fact_sales', dw_engine,
                          if_exists='append', index=False)

############################################
# EXTRACT
############################################


# Get last id from fact_sales data warehouse
last_id = get_factSales_last_id(dw_engine)
logger.debug('last_id={}'.format(last_id))

# Extract the payment table into a pandas DataFrame
payment_df = extract_table_payment(last_id, db_engine)

# If no records fetched, then exit
if payment_df.shape[0] == 0:
    raise Exception('No new record in source table')

# Extract lookup table `customer`
dim_customer_df = lookup_dim_customer(payment_df, dw_engine)

# Extract lookup table `rental`
rental_df = lookup_table_rental(payment_df, db_engine)

# Extract lookup table `inventory`
inventory_df = lookup_table_inventory(rental_df, db_engine)

# Extract lookup table `dim_movie`
dim_movie_df = lookup_dim_movie(inventory_df, dw_engine)

# Extract lookup table `dim_store`
dim_store_df = lookup_dim_store(inventory_df, dw_engine)

############################################
# TRANSFORM
############################################

# Join table `payment` & `dim_customer`
dim_payment_df = join_payment_dim_customer(payment_df, dim_customer_df)
logger.debug('dim_payment_df=\n{}'.format(dim_payment_df))

# Join table `payment` & `rental`
dim_payment_df = join_payment_rental(dim_payment_df, rental_df)
logger.debug('dim_payment_df=\n{}'.format(dim_payment_df))

# Join table `payment` & `inventory`
dim_payment_df = join_payment_inventory(dim_payment_df, inventory_df)
logger.debug('dim_payment_df=\n{}'.format(dim_payment_df))

# Join table `payment` & `dim_movie`
dim_payment_df = join_payment_dim_movie(dim_payment_df, dim_movie_df)
logger.debug('dim_payment_df=\n{}'.format(dim_payment_df))

# Join table `payment` & `dim_store`
dim_payment_df = join_payment_dim_store(dim_payment_df, dim_store_df)
logger.debug('dim_payment_df=\n{}'.format(dim_payment_df))

# Add date_key smart_key
dim_payment_df = add_date_key(dim_payment_df)
logger.debug('dim_payment_df=\n{}'.format(dim_payment_df))

# Rename and remove
dim_payment_df = rename_remove_columns(dim_payment_df)
logger.debug('dim_payment_df=\n{}'.format(dim_payment_df))

# Validate result
dim_payment_df = validate(payment_df, dim_payment_df)
logger.debug('dim_payment_df=\n{}'.format(dim_payment_df.dtypes))

############################################
# LOAD
############################################

# Load dimension table `fact_sales`
load_dim_payment(dim_payment_df)
