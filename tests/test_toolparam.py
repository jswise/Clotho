import pathlib
import pytest
import shutil
import sqlite3

import pandas as pd

from clotho.clothodb import ClothoDB
from clotho.logutils import start_logging
from clotho.resourceshed import ResourceShed
from clotho.tool import Tool
from clotho.toolparam import ToolParam

CONFIG_NAME = 'ClothoConfig.yaml'
DATA_FOLDER = pathlib.Path(__file__).parents[1] / 'data'
INPUT_FOLDER = DATA_FOLDER / 'input'
INPUT_CONFIG = INPUT_FOLDER / CONFIG_NAME
OUTPUT_FOLDER = DATA_FOLDER / 'output'
OUTPUT_CONFIG = OUTPUT_FOLDER / CONFIG_NAME
DB_PATH = OUTPUT_FOLDER / 'TestDB.sqlite'

PARAM1_ID = 'bogus1'
PARAM2_ID = 'bogus2'
TOOL_ID = 'bogustool'

CLOTHO_FOLDER = INPUT_FOLDER / 'SkiWX' / 'ClothoDB'
OUTPUT_CLOTHO_DB = OUTPUT_FOLDER / 'SkiWX' / 'ClothoDB.sqlite'

CONFIG_1 = {
    'ToolID': TOOL_ID,
    'ParamID': PARAM1_ID,
    'Name': 'endpoint',
    'Value': 'https://devapi.terracon.com/employees/'
}
CONFIG_2 = {
    'ToolID': TOOL_ID,
    'ParamID': PARAM2_ID,
    'Name': 'output_csv',
    'Value': 'C:/Temp/Employees.csv'
}


def create_db():
    if OUTPUT_CLOTHO_DB.exists():
        OUTPUT_CLOTHO_DB.unlink()
    OUTPUT_CLOTHO_DB.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(OUTPUT_CLOTHO_DB)
    for csv in CLOTHO_FOLDER.glob('*.csv'):
        df = pd.read_csv(csv)
        df.to_sql(csv.stem, connection, if_exists='replace', index=False)
    connection.close()


def write_params():
    db = ClothoDB(DB_PATH)
    db.build_schema()
    db.set_row('ToolParams', CONFIG_1, 'ParamID')
    db.set_row('ToolParams', CONFIG_2, 'ParamID')


@pytest.fixture(scope='module', autouse=True)
def setup():
    start_logging()
    create_db()
    write_params()


@pytest.fixture
def db():
    return ClothoDB(DB_PATH)


def get_extractor_gz():
    tool_id = Tool(OUTPUT_CLOTHO_DB, {'Name': 'Extract Weather'}).id
    shed = ResourceShed(OUTPUT_CLOTHO_DB)
    return ToolParam(
        OUTPUT_CLOTHO_DB,
        {'Name': 'gz_file', 'ToolID': tool_id},
        resource_shed=shed
    )


@pytest.fixture
def extractor_gz():
    return get_extractor_gz()


@pytest.fixture
def extractor_csv():
    tool_id = Tool(OUTPUT_CLOTHO_DB, {'Name': 'Extract Weather'}).id
    shed = ResourceShed(OUTPUT_CLOTHO_DB)
    return ToolParam(
        OUTPUT_CLOTHO_DB,
        {'Name': 'output_path', 'ToolID': tool_id},
        resource_shed=shed
    )

@pytest.fixture
def victim(db):
    return ToolParam(db, id=PARAM1_ID)


def test_init(db):
    param = ToolParam(db, id=PARAM1_ID)
    assert 'devapi' in param.config['Value']

    param = ToolParam(db, config={'ToolID': TOOL_ID, 'name': 'output_csv'})
    assert param.config['ParamID'] == PARAM2_ID

    param = ToolParam(db)
    assert param.config['ParamID'] is None


def test_commit(db):
    row = db.get('ToolParams', """ "ParamID" == '{}' """.format(PARAM1_ID))
    assert row['Name'].iloc[0] == 'endpoint'

    param = ToolParam(db, config={'ToolID': TOOL_ID, 'name': 'EndPoint'})
    param.commit(TOOL_ID)

    row = db.get('ToolParams', """ "ParamID" == '{}' """.format(PARAM1_ID))
    assert row['Name'].iloc[0] == 'EndPoint'

    param = ToolParam(db, config={'ToolID': TOOL_ID, 'name': 'query'})
    param.commit(TOOL_ID)

    row = db.get('ToolParams', """ "Name" == 'query' """)
    assert row['ParamID'].iloc[0]


def test_configure(victim):
    victim.configure(CONFIG_2)
    assert victim.id == PARAM2_ID


def test_get_resource(extractor_gz):
    assert extractor_gz.get_resource().name == 'METAR GZ'


def test_id(victim):
    assert victim.id == PARAM1_ID


def test_is_input(extractor_gz, extractor_csv):
    assert extractor_gz.is_input
    assert not extractor_csv.is_input


def test_is_read(extractor_gz, extractor_csv):
    assert extractor_gz.is_read


def test_is_write(extractor_gz, extractor_csv):
    assert extractor_csv.is_write


def test_record_io(extractor_gz):
    extractor_gz.record_io('Activity1', 'My.gz')
    db = ClothoDB(OUTPUT_CLOTHO_DB)
    row = db.get_row('ActivityIO', 'ActivityID', 'Activity1')
    assert row['Value'] == 'My.gz'


def test_value(extractor_gz):
    assert extractor_gz.value is None
    extractor_gz.value = 'asdf'
    assert extractor_gz.value.endswith('ClothoDemo/asdf')


if __name__ == '__main__':
    start_logging()
    create_db()
    write_params()
    test_init(ClothoDB(DB_PATH))
    test_value(get_extractor_gz())
