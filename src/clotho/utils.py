from datetime import datetime
from datetime import timedelta
from datetime import timezone
import logging
import pathlib
import sqlite3

import yaml
import pandas as pd

from clotho.errors import ClothoError
from clotho.logutils import raise_error


def condition(column, value):
    """Build a condition string in the form "column == 'value'".

    :param column: The name of a table column to search
    :param value: A string or number to search for

    :return: A condition string in the form "column == 'value'"
    """

    return "{} == {}".format(column, quote_val(value))


def extract_batch_ids(data_path, db_creds=None):
    """Given the location of output data, find the data's batch IDs.

    :param data_path: The file or database path to data generated by a Clotho tool
    :param db_creds: A dictionary of database credentials, if the data are in an RDBMS

    :return: A list of batch IDs
    """

    data_path = pathlib.Path(data_path)
    if str(data_path.parent.suffix).lower() == '.sqlite':
        return extract_sqlite_batch_ids(data_path)
    # if not data_path.anchor:
    #     return extract_rdbms_batch_ids(data_path, db_creds)
    return extract_file_batch_ids(data_path)


def extract_file_batch_ids(data_path):
    """Given the location of a data file, find the file's batch IDs.

    :param data_path: The path to a file generated by a Clotho tool

    :return: A list of batch IDs
    """

    metadata_file = data_path.parent / (data_path.name + '.clotho')
    try:
        if metadata_file.exists():
            with open(metadata_file, 'r') as stream:
                metadata = yaml.load(stream, Loader=yaml.FullLoader)
                batch_id = metadata.get('BatchID')
                if batch_id is None:
                    return []
                else:
                    return [batch_id]
    except OSError:
        pass
    if str(data_path.suffix).lower() == '.csv':
        df = pd.read_csv(data_path)
        if 'BatchID' in df.columns:
            return list(df.BatchID.unique())
    return []


def extract_sqlite_batch_ids(data_path):
    """Given the location of a table in a SQLite file, find the table's batch IDs.

    :param data_path: The path to a SQLite table generated by a Clotho tool

    :return: A list of batch IDs
    """

    conn = sqlite3.connect(data_path.parent)
    df = pd.read_sql(f'SELECT * FROM {data_path.name}', conn)
    # shard = SQLiteShard(data_path.parent, 'clotho')
    # df = shard.get(data_path.name)
    if df is None:
        return []
    return list(df.BatchID.unique())


def fill_config(db, table_name, id_column, config=None, id=None, name_column=None):
    """Use the database to fill in missing values in a config.

    You must provide a unique ID using either the config or id parameter.

    :param db: A ClothoDB
    :param table_name: The name of the table to search
    :param id_column: The unique ID column in the table
    :param config: A configuration dictionary, which may be incomplete
    :param id: The unique ID to search for
    :param name_column: The name of a column to try if we don't find the ID

    :return: A dictionary containing the updated configuration
    """

    # Get the ID.
    if config is None:
        config = {}
    id = get_case_insensitive(config, id_column, id)

    # Try getting the row from the database by ID.
    if id is None:
        row = None
    else:
        row = db.get_row_insensitive(table_name, id_column, id)

    # If that didn't work, try getting it by name.
    if row is None and name_column:
        name = get_case_insensitive(config, name_column)
        if name:
            row = db.get_row_insensitive(table_name, name_column, name)

    # If we still haven't found it, make a blank dictionary.
    if row is None:
        columns = db.get_columns(table_name)
        row = dict(zip(columns, [None] * len(columns)))

    # Replace values from the database with provided values.
    for key, db_val in row.items():
        val = get_case_insensitive(config, key, db_val)
        if isinstance(val, pathlib.Path):
            val = str(val)
        row[key] = val

    return row


def fill_nans(df):
    """Replace NaNs with appropriate values."""

    values = {}
    for col_name, dtype in df.dtypes.items():
        lower_type = str(dtype).lower()
        if str(dtype) == 'bool':
            values[col_name] = False
        elif ('int' in lower_type) or ('float' in lower_type):
            values[col_name] = 0
        else:
            values[col_name] = ''
    return df.fillna(values)


def get_case_insensitive(dictionary, key, default_value=None):
    """A case-insensitive way to get a value from a dictionary.

    :param dictionary: The dictionary to search.
    :param key: The key to look up, not case-sensitive
    :param default_value: The value to return if the key isn't found

    :return: The found or default value
    """

    lower_name = key.lower()
    for key, val in dictionary.items():
        if key.lower() == lower_name:
            return val

    return default_value


def get_flex(dictionary, keys, synonyms=None, default_value=None):
    """Look in a dictionary for any of multiple, non-case-sensitive keys.

    :param dictionary: The dictionary to search.
    :param keys: A list of possible keys to look up, not case-sensitive
    :param default_value: The value to return if none of the keys are found

    :return: The found or default value
    """

    if not isinstance(keys, list):
        keys = [keys]
    if not synonyms:
        synonyms = {}

    # Add synonyms to the list of keys.
    all_keys = []
    for key in keys:
        all_keys.append(key)
        synonym = get_case_insensitive(synonyms, key)
        if synonym:
            all_keys.append(synonym)

    # Search for all the keys.
    for key in all_keys:
        val = get_case_insensitive(dictionary, key)
        if val:
            return val

    return default_value


def get_now():
    return datetime.now(timezone.utc)


def get_time_string(time_obj):
    if isinstance(time_obj, datetime):
        return time_obj.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(time_obj, timedelta):
        return str(time_obj).split('.', 2)[0]
    raise_error('Attempted to convert unrecognized time object.', ClothoError)


def get_schema_templates():
    """Get empty tables for the Clotho schema.

    :return: A dictionary of table names and empty tables
    """

    # Get the table configurations from this library's YAML file.
    yaml_path = pathlib.Path(__file__).parent / 'ClothoSchema.yaml'
    with open(yaml_path, 'r') as stream:
        content = yaml.load(stream, Loader=yaml.FullLoader)
    schema = content.get('tables')

    # Create the tables.
    tables = {}
    for table_name, column_types in schema.items():
        columns = {}
        for col_name, col_type in column_types.items():
            if 'datetime' in col_type:
                col_type = 'datetime64[ns]'
            columns[col_name] = pd.Series(dtype=col_type)
        tables[table_name] = pd.DataFrame(columns)
    return tables


def quote_val(val):
    """See if a value is a string, and if so, put single quotes around it.

    :param val: A string or number

    :return: The same value, but with single quotes if it's a string
    """

    if isinstance(val, str):
        return "'{}'".format(val)
    return str(val)


def raise_error(message):
    logging.error(message)
    raise ClothoError(message)
