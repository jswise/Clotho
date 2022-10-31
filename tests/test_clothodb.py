from importlib_metadata import pathlib
import pytest
import sqlite3

import pandas as pd

from clotho.clothodb import ClothoDB
from clotho.errors import ClothoError
from clotho.logutils import start_logging
import dbtesthelpers
import test_sqlitedb


def next_fruit_id(victim):
    df = victim.get('Fruit')
    return df.FruitID.max() + 1


def create_fruit_table(source):
    df = pd.DataFrame(
        {
            'FruitID': [0, 1, 2],
            'Name': ['apple', 'banana', 'acorn'],
            'Color': ['green', 'yellow', 'brown']
        }
    )
    conn = sqlite3.connect(source)
    df.to_sql('Fruit', conn, if_exists='replace', index=False)


@pytest.fixture(scope='module', autouse=True)
def setup():
    start_logging()
    test_sqlitedb.create_db()


@pytest.fixture
def victims():
    victims = [
        ClothoDB(test_sqlitedb.DB)
    ]
    return victims


def test_build_schema(victims):
    for victim in victims:
        victim.build_schema()
        assert 'Resources' in victim.table_names


def test_delete_row(victims):
    for victim in victims:
        fruit = victim.get('Fruit')
        assert not fruit[fruit.Name == 'acorn'].empty
        victim.delete_row('Fruit', 'Name', 'acorn')
        fruit = victim.get('Fruit')
        assert fruit[fruit.Name == 'acorn'].empty


def test_get(victims):
    for victim in victims:
        dbtesthelpers.test_get(victim)


def test_get_columns(victims):
    for victim in victims:
        assert 'Name' in victim.get_columns('Fruit')


def test_get_row(victims):
    for victim in victims:
        row = victim.get_row('Fruit', 'FruitID', 0)
        assert 'Name' in row
        row = victim.get_row('Fruit', 'Name', 'banana')
        assert 'Name' in row
        row = victim.get_row('Fruit', 'Name', 'bAnana')
        assert row is None
        row = victim.get_row('Fruit', 'Name', 'forbidden')
        assert row is None
        id = next_fruit_id(victim)
        new_row = {'FruitID': id, 'Name': 'apple', 'Color': 'red'}
        victim.set_row('Fruit', new_row, 'FruitID')
        with pytest.raises(ClothoError):
            victim.get_row('Fruit', 'Name', 'apple')


def test_get_row_insensitive(victims):
    for victim in victims:
        for name in ['banana', 'Banana']:
            row = victim.get_row_insensitive('Fruit', 'Name', name)
            assert 'Name' in row
        row = victim.get_row_insensitive('Fruit', 'Name', 'mango')
        assert row is None
        id = next_fruit_id(victim)
        new_row = {'FruitID': id, 'Name': 'cherry', 'Color': 'red'}
        victim.set_row('Fruit', new_row, 'FruitID')
        new_row = {'FruitID': id + 1, 'Name': 'cherry', 'Color': 'black'}
        victim.set_row('Fruit', new_row, 'FruitID')
        with pytest.raises(ClothoError):
            victim.get_row_insensitive('Fruit', 'Name', 'cherry')


def test_import_df(victims):
    for victim in victims:
        dbtesthelpers.test_import_df(victim)


def test_init_db(victims):
    victims[0].init_db(test_sqlitedb.DB)
    source = victims[0]._db.source
    assert pathlib.Path(source).is_file()


def test_set_row(victims):
    for victim in victims:
        id = next_fruit_id(victim)
        row = {'FruitID': id, 'Name': 'grape', 'Color': 'red'}
        victim.set_row('Fruit', row, 'FruitID')
        assert victim.get_row('Fruit', 'FruitID', id)['Color'] == 'red'
        row['Color'] = 'purple'
        victim.set_row('Fruit', row, 'FruitID')
        assert victim.get_row('Fruit', 'FruitID', id)['Color'] == 'purple'


def test_table_names(victims):
    for victim in victims:
        dbtesthelpers.test_table_names(victim)


def test_update(victims):
    for victim in victims:
        vals = {'Color': 'red'}
        victim.update('Fruit', 'FruitID', 0, vals)
        apples = victim.get('Fruit', """ "FruitID" == 0 """)
        assert len(apples) == 1
        assert apples['Name'][0] == 'apple'
        assert apples['Color'][0] == 'red'
