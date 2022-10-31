import os
import pathlib
import sqlite3

import pandas as pd
import pytest

from clotho.logutils import start_logging
from clotho.sqlitedb import SQLiteDB
import dbtesthelpers

DATA_FOLDER = pathlib.Path(__file__).parents[1] / 'data'
INPUT_FOLDER = DATA_FOLDER / 'input'
OUTPUT_FOLDER = DATA_FOLDER / 'output'
DB = OUTPUT_FOLDER / 'TestDB.sqlite'


@pytest.fixture
def victim():
    return SQLiteDB(DB, 'clotho')


def create_db():
    if DB.exists():
        DB.unlink()
    df = pd.DataFrame(
        {
            'FruitID': [0, 1, 2],
            'Name': ['apple', 'banana', 'acorn'],
            'Color': ['green', 'yellow', 'brown']
        }
    )
    connection = sqlite3.connect(DB)
    df.to_sql('clotho.Fruit', connection, if_exists='replace', index=False)
    connection.close()


@pytest.fixture(scope='module', autouse=True)
def setup():
    start_logging()
    create_db()


def test_delete_row(victim):
    assert not victim.get('Fruit', "Name == 'acorn'").empty
    victim.delete_row('Fruit', 'Name', 'acorn')
    assert victim.get('Fruit', "Name == 'acorn'").empty


def test_get(victim):
    dbtesthelpers.test_get(victim)


def test_get_columns(victim):
    assert 'Name' in victim.get_columns('Fruit')


def test_get_connection(victim):
    victim._get_connection()


def test_import_df(victim):
    dbtesthelpers.test_import_df(victim)


def test_table_names(victim):
    dbtesthelpers.test_table_names(victim)


def test_update(victim):
    vals = {'Color': 'red'}
    victim.update('Fruit', 'FruitID', 0, vals)
    apples = victim.get('Fruit', """ "FruitID" == 0 """)
    assert len(apples) == 1
    assert apples['Name'][0] == 'apple'
    assert apples['Color'][0] == 'red'
