import pathlib
import pytest
import sqlite3

import pandas as pd

from clotho.clothodb import ClothoDB
from clotho import utils
from clotho.logutils import start_logging


OUTPUT_FOLDER = pathlib.Path(__file__).parents[1] / 'data' / 'output'
CSV = OUTPUT_FOLDER / 'clotho.Fruit.csv'
CLOTHO_DB = OUTPUT_FOLDER / 'TestDB.sqlite'
FRUIT_DB = OUTPUT_FOLDER / 'Fruit.sqlite'
RESOURCE_ID = '64a3f200-d77e-11ec-a1b8-a44cc83e0d72'


def write_data():
    df = pd.DataFrame({'A': [1, 2], 'BatchID': ['batch 1', 'batch 2']})
    df.to_csv(CSV, index=False)
    conn = sqlite3.connect(FRUIT_DB)
    df.to_sql('Fruit', conn, if_exists='replace', index=False)


def write_resources():
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    db = ClothoDB(CLOTHO_DB)
    db.build_schema()
    db.set_row(
        'Resources',
        {'Name': 'X Drive', 'Path': 'X:', 'ResourceID': RESOURCE_ID},
        'ResourceID'
    )


def check_batch_ids(batch_ids):
    assert batch_ids[0] == 'batch 1'
    assert batch_ids[1] == 'batch 2'


@pytest.fixture(scope='module', autouse=True)
def setup():
    start_logging()
    write_resources()
    write_data()


def test_condition():
    assert utils.condition('A', 1) == 'A == 1'
    assert utils.condition('A', '1') == "A == '1'"


def test_extract_batch_ids():
    batch_ids = utils.extract_batch_ids(CSV)
    check_batch_ids(batch_ids)

    batch_ids = utils.extract_sqlite_batch_ids(FRUIT_DB / 'Fruit')
    check_batch_ids(batch_ids)


def test_extract_file_batch_ids():
    batch_ids = utils.extract_file_batch_ids(CSV)
    check_batch_ids(batch_ids)


def test_extract_sqlite_batch_ids():
    batch_ids = utils.extract_sqlite_batch_ids(FRUIT_DB / 'Fruit')
    check_batch_ids(batch_ids)


def test_fill_config():
    db = ClothoDB(CLOTHO_DB)
    kwargs = {
        'db': db,
        'table_name': 'Resources',
        'id_column': 'ResourceID',
    }
    config = utils.fill_config(**kwargs)
    assert config['Name'] is None

    config = utils.fill_config(**kwargs, id=RESOURCE_ID)
    assert config['Name'] == 'X Drive'

    config = utils.fill_config(**kwargs, config={'resourceid': RESOURCE_ID})
    assert config['Name'] == 'X Drive'


def test_get_case_insensitive():
    d = {'A': 1}
    assert utils.get_case_insensitive(d, 'a') == 1
    assert utils.get_case_insensitive(d, 'A') == 1
    assert utils.get_case_insensitive(d, 'b') is None


def test_get_flex():
    d = {'A': 1}
    assert utils.get_flex(d, ['a', 'b'])
    assert utils.get_flex(d, 'a')
    assert utils.get_flex(d, 'b') is None
    assert utils.get_flex(d, 'b', {'b': 'a'})
    assert utils.get_flex(d, 'b', {'B': 'a'})


def test_get_now():
    now = utils.get_now()
    now_string = utils.get_time_string(now)
    assert now_string.startswith('20')


def test_quote_val():
    assert utils.quote_val(1) == '1'
    assert utils.quote_val('1') == "'1'"
