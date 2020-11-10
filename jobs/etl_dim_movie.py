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


def get_dimMovie_last_id(database):
    """ Function to get last film_id from dimension table `dim_movie` """
    query = "SELECT max(film_id) AS last_film_id FROM dim_movie"
    tdf = pd.read_sql(query, database)
    return tdf.iloc[0]['last_film_id']


def extract_table_film(last_film_id, database):
    """ Function to extract table `film` """
    if last_film_id == None:
        last_film_id = -1

    query = "SELECT * FROM film WHERE film_id > {} LIMIT 100000".format(
        last_film_id)
    return pd.read_sql(query, database)


def lookup_table_language(film_df, database):
    """ Funstion to lookup table `language` """
    unique_ids = list(film_df.language_id.unique())
    unique_ids = list(filter(None, unique_ids))

    query = "SELECT * FROM language WHERE language_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, database)


def join_film_language(film_df, language_df):
    """ Transformation: join table `film` and `language` """
    film_df = pd.merge(film_df, language_df, left_on='language_id',
                       right_on='language_id', how='left')
    film_df = film_df[['film_id', 'title', 'description', 'release_year',
                       'name', 'rental_duration', 'length', 'rating', 'special_features']]
    film_df = film_df.rename({'name': 'language'}, axis=1)
    return film_df


def validate(source_df, destination_df):
    """ Function to validate transformation result """
    source_row_count = source_df.shape[0]
    destination_row_count = destination_df.shape[0]

    if(source_row_count == destination_row_count):
        return destination_df
    else:
        raise ValueError(
            'Transormation result is not valid: row count is not equal'
        )


def load_dim_movie(destiantion_df):
    """ Load to data warehouse """
    destiantion_df.to_sql('dim_movie', dw_engine,
                          if_exists='append', index=False)

############################################
# EXTRACT
############################################


# Get last film_id from dim_movie data warehouse
last_film_id = get_dimMovie_last_id(dw_engine)
logger.debug('last_film_id={}'.format(last_film_id))

# Extract the store table into a pandas DataFrame
film_df = extract_table_film(last_film_id, db_engine)

# If no records fetched, then exit
if film_df.shape[0] == 0:
    raise Exception('No new record in source table')

# Extract lookup table `language`
language_df = lookup_table_language(film_df, db_engine)

############################################
# TRANSFORM
############################################

# Join table `film` with `language`
dim_movie_df = join_film_language(film_df, language_df)

# Validate result
dim_movie_df = validate(film_df, dim_movie_df)

############################################
# LOAD
############################################

# Load dimension table `dim_movie`
load_dim_movie(dim_movie_df)
