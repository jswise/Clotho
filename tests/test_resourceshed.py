from importlib.resources import Resource
import pathlib
import pytest
import sqlite3

import pandas as pd

from clotho.clothodb import ClothoDB
from clotho.logutils import start_logging
from clotho.resourceshed import ResourceShed
from clotho.toolparam import ToolParam

DATA_FOLDER = pathlib.Path(__file__).parents[1] / 'data'
INPUT_FOLDER = DATA_FOLDER / 'input'
CLOTHO_FOLDER = INPUT_FOLDER / 'SkiWX' / 'ClothoDB'
OUTPUT_FOLDER = DATA_FOLDER / 'output'
DB = OUTPUT_FOLDER / 'TestDB.sqlite'

def create_db():
    if DB.exists():
        DB.unlink()
    DB.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB)
    for csv in CLOTHO_FOLDER.glob('*.csv'):
        df = pd.read_csv(csv)
        df.to_sql(csv.stem, connection, if_exists='replace', index=False)
    connection.close()


@pytest.fixture(scope='module', autouse=True)
def setup():
    start_logging()
    create_db()

@pytest.fixture
def victim():
    return ResourceShed(DB)

def test_get(victim):
    assert not victim.resources
    metar_csv = victim.get('METAR CSV')
    assert metar_csv.name == 'METAR CSV'
    assert metar_csv.path is None
    assert 'METAR CSV' in victim.resources

if __name__ == '__main__':
    start_logging()
    create_db()
    test_get(ResourceShed(DB))
