"""Define the ClothDB class."""

import pathlib

import numpy as np
import pandas as pd

import clotho.logutils
from clotho.errors import ClothoError
from clotho.sqlitedb import SQLiteDB
from clotho.utils import get_schema_templates
from clotho.utils import fill_nans


class ClothoDB:
    """Represents a Clotho database."""

    def __init__(self, source):
        """Represents the database for Clotho configuration & metadata.

        The source can be a path to a SQLite database file (which doesn't need to exist yet),
        a path to a folder for storing CSV files (which also doesn't need to exist yet),
        or a dictionary containing "server" (a SQL server machine), "database" (the name of an
        existing SQL Server database), "uid" (a user ID), "pwd" (a password), and optionally
        the name of a driver (the default is "SQL+Server").

        :param source: The location or connection information for a database
        """

        self._db = None
        self.schema = 'clotho'
        self.init_db(source)

    def build_schema(self):
        """Create the Clotho schema in the database."""

        # See if each table already exists.
        table_names = self.table_names
        for table_name, df in get_schema_templates().items():
            if table_name in table_names:
                continue

            # Create each table.
            self.import_df(df, table_name)

    def delete_row(self, table_name, key, value):
        """Delete any rows that match the key/value pair.

        :param table_name: The name of the table containing the row
        :param key: The name of the column to search
        :param value: The value to search for in the column
        """

        self._db.delete_row(table_name, key, value)

    def get(self, table_name, query=None):
        """Get a table from the database.

        :param table_name: The name of a table in a database
        :param query: An optional query to filter the data

        :return: A Pandas dataframe containing the table's data
        """

        df = self._db.get(table_name, query)
        if df is None:
            return
        return df.replace(np.nan, None)

    def get_columns(self, table_name):
        """Get a list of column names for a table.

        :param table_name: The name of the table whose columns we want

        :return: A list of column names
        """

        return self._db.get_columns(table_name)

    def get_row(self, table_name, key, value):
        """Get a specific row from a table.

        :param table_name: The name of the table containing the row
        :param key: The name of the column to search
        :param value: The value to search for in the column

        :return: A dictionary containing the data from a table row
        """

        if isinstance(value, str):
            query = """ "{}" == '{}' """.format(key, value)
            if self.type == 'SQL Server':
                query += ' COLLATE SQL_Latin1_General_CP1_CS_AS'
        else:
            query = """ "{}" == {} """.format(key, value)
        df = self._db.get(table_name, query)
        if df is None:
            return
        rows = df.to_dict('records')
        if len(rows) > 1:
            clotho.logutils.raise_error(
                'Multiple rows in {} for {}.'.format(table_name, query),
            )
        if len(rows) == 1:
            return rows[0]

    def get_row_insensitive(self, table_name, key, value):
        """Get a table row by searching for a string, without worrying about capitalization.

        :param table_name: The name of the table containing the row
        :param key: The name of the column to search
        :param value: The value to search for in the column (regardless of capitalization)

        :return: A dictionary containing the data from a table row
        """

        df = self._db.get(table_name)
        if df is None:
            return
        df = df[df[key].str.lower() == str(value).lower()]
        df = df.replace({np.nan: None})
        rows = df.to_dict('records')
        if len(rows) > 1:
            clotho.logutils.raise_error(
                'Multiple rows in {} for {} == {}.'.format(table_name, key, value),
            )
        if len(rows) == 1:
            return rows[0]

    def import_df(self, df, table_name, overwrite=False):
        """Create or replace a table based on a Pandas dataframe.

        :param df: A Pandas dataframe
        :param table_name: The name of the new database table
        :param overwrite: A boolean indicating whether to overwrite or append an existing table
        """

        df = fill_nans(df)
        self._db.import_df(df, table_name, overwrite)

    def init_db(self, source):
        """Instantiate the appropriate object for the database format.

        :param source: A dictionary containing server, database, uid, pwd, and driver
        """

        if isinstance(source, str):
            source = pathlib.Path(source)
        if isinstance(source, pathlib.Path):
            if source.suffix.lower() == '.sqlite':
                self._db = SQLiteDB(source, self.schema)
                return
        clotho.logutils.raise_error(f'Unrecognized source type: {source}')

    def set_row(self, table_name, row, id_col):
        """Create or replace a row in a table.

        :param table_name: The name of a table in the database
        :param row: A dictionary to define the new row
        :param id_col: The name of the unique ID column
        """

        # Get the existing table, minus the row we're replacing.
        df = self._db.get(table_name)
        if df is None:
            df = pd.DataFrame()
        else:
            df = df[df[id_col] != row[id_col]]

        # Make the input row into a dataframe.
        template = get_schema_templates().get(table_name)
        if template is not None:
            new_row = template.copy(True)
            new_row.loc[0, row.keys()] = list(row.values())
            new_row = new_row.astype(template.dtypes.to_dict())
            df = df.astype(template.dtypes.to_dict())
        else:
            new_row = pd.DataFrame([row])

        # Put the old data plus the new row in the database.
        df = pd.concat([df, new_row], ignore_index=True)
        self._db.import_df(df, table_name, True)

    @property
    def table_names(self):
        return self._db.table_names

    @property
    def type(self):
        return self._db.type

    def update(self, table_name, query_col, query_val, vals):
        """Replace cells in a table while leaving other cells alone.

        :param table_name: The name of a table in the database
        :param query_col: The name of the column to search (typically the unique ID column)
        :param query_val: The value to search for (typically a unique ID)
        :param vals: A dictionary containing the unique ID and values to change
        """

        self._db.update(table_name, query_col, query_val, vals)
