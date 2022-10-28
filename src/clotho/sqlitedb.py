"""Define the SQLiteDB class."""

import logging
import pathlib
import sqlite3
from sqlite3.dbapi2 import OperationalError

import pandas as pd

from clotho.utils import condition
from clotho.utils import quote_val


class SQLiteDB:
    """Represents a SQLite database."""

    def __init__(self, source, schema=None) -> None:
        self.schema = schema
        self.source = pathlib.Path(source)

    def prepend_schema(self, table_name) -> str:
        """Prepend the schema name to the table name.

        :param table_name: The name of a table in the database

        :return: The table name in schema.name format
        """

        if self.schema:
            return self.schema + '.' + table_name
        return table_name

    def delete_row(self, table_name, key, value):
        """Delete any rows that match the key/value pair.

        :param table_name: The name of the table containing the row
        :param key: The name of the column to search
        :param value: The value to search for in the column
        """

        if isinstance(value, str):
            query = "{} = '{}'".format(key, value)
        else:
            query = "{} = {}".format(key, value)
        table_name = self.prepend_schema(table_name)
        connection = self._get_connection()
        connection.execute('DELETE FROM [{}] WHERE {}'.format(table_name, query))
        connection.commit()

    def get(self, table_name, query=None):
        """Get a table from the database.

        :param table_name: The name of a table in a database
        :param query: An optional query to filter the data

        :return: A Pandas dataframe containing the table's data
        """

        table_name = self.prepend_schema(table_name)
        sql = "SELECT * FROM '{}'".format(table_name)
        if query:
            sql += " WHERE {}".format(query)
        connection = self._get_connection()
        try:
            df = pd.read_sql(sql, connection)
        except Exception as e:
            logging.debug('Failed to read {}. {}'.format(table_name, e))
            df = None
        connection.close()

        return df

    def get_columns(self, table_name):
        """Get a list of column names for a table.

        :param table_name: The name of the table whose columns we want

        :return: A list of column names
        """

        df = self.get(table_name, '0 == 1')
        return df.columns

    def _get_connection(self):
        """Connect to the database."""

        try:
            return sqlite3.connect(self.source)
        except Exception as e:
            logging.error('Failed to connect to {}. {}'.format(self.source, e))

    def import_df(self, df, table_name, overwrite=False):
        """Create or replace a table based on a Pandas dataframe.

        :param df: A Pandas dataframe
        :param table_name: The name of the new database table
        :param overwrite: A boolean indicating whether to overwrite or append an existing table
        """

        if overwrite:
            if_exists = 'replace'
        else:
            if_exists = 'append'

        connection = self._get_connection()

        # Write the table to the database.
        table_name = self.prepend_schema(table_name)
        try:
            df.to_sql(table_name, connection, if_exists=if_exists, index=False)

        # We may have a table with more columns than the one in the database.
        except OperationalError as op_error:
            try:
                cursor = self.connection.execute('SELECT * FROM [{}]'.format(table_name))
                col_names = [description[0].lower() for description in cursor.description]
                cursor = self.connection.cursor()
                for col_name in df.columns:
                    if not col_name.lower() in col_names:
                        cursor.execute(
                            "ALTER TABLE [{}] ADD COLUMN '{}'".format(
                                table_name,
                                col_name
                            )
                        )
                df.to_sql(table_name, self.connection, if_exists='append', index=False)
            except Exception as e:
                logging.warning('Failed to write {}. {}\n{}'.format(table_name, op_error, e))
        except Exception as e:
            logging.warning('Failed to write {}. {}'.format(table_name, e))
        connection.close()

    @property
    def table_names(self):
        """Return a list of all the tables in the database.

        :return: A list of table names
        """

        connection = self._get_connection()
        metadf = pd.read_sql_query(
                "SELECT name FROM sqlite_master WHERE type='table'",
                connection
            )
        connection.close()
        raw_names = list(metadf['name'])
        if not self.schema:
            return raw_names
        table_names = []
        for raw_name in raw_names:
            table_names.append('.'.join(raw_name.split('.')[1:]))
        return table_names

    @property
    def type(self):
        return 'SQLite'

    def update(self, table_name, query_col, query_val, vals):
        """Replace cells in a table while leaving other cells alone.

        :param table_name: The name of a table in the database
        :param query_col: The name of the column to search (typically the unique ID column)
        :param query_val: The value to search for (typically a unique ID)
        :param vals: A dictionary containing the unique ID and values to change
        """

        pairs = []
        for col, val in vals.items():
            if val is None:
                continue
            pairs.append("{} = {}".format(col, quote_val(val)))
        pair_string = ', '.join(pairs)

        table_name = self.prepend_schema(table_name)
        query = 'UPDATE [{}] SET {} WHERE {}'.format(
            table_name,
            pair_string,
            condition(query_col, query_val)
        )
        connection = self._get_connection()
        connection.execute(query)
        connection.commit()
