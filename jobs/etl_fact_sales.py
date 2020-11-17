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
    
    query = "SELECT * FROM rental WHERE rental_id IN ({})".format(','.join(map(str, unique_ids)))
    return pd.read_sql(query, db_engine)
