import pathlib
import pytest
import shutil

from clotho.clothoconfig import ClothoConfig
from clotho.clothodb import ClothoDB
from clotho.logutils import start_logging

DATA_FOLDER = pathlib.Path(__file__).parents[1] / 'data'
INPUT_MAIN = DATA_FOLDER / 'input' / 'ClothoConfig.yaml'
INPUT_SIMPLE = DATA_FOLDER / 'input' / 'SimpleConfig.yaml'
OUTPUT_FOLDER = DATA_FOLDER / 'output'
OUTPUT_MAIN = OUTPUT_FOLDER / 'ClothoConfig.yaml'
OUTPUT_SIMPLE = DATA_FOLDER / 'output' / 'SimpleConfig.yaml'
DB = OUTPUT_FOLDER / 'TestDB.sqlite'


def build_db():
    copy_config()
    # os.chdir(OUTPUT_FOLDER)
    if DB.exists():
        DB.unlink()
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    db = ClothoDB(DB)
    db.build_schema()
    db.set_row('Tools', {'ToolID': 'tool00', 'Name': 'Do Nothing'}, 'ToolID')
    db.set_row('Tools', {'ToolID': 'tool01', 'Name': 'Get Data'}, 'ToolID')
    db.set_row('Tools', {'ToolID': 'tool02', 'Name': 'Crunch Data'}, 'ToolID')
    db.set_row(
        'ToolPredecessors',
        {
            'RelationshipID': 'rel01',
            'ToolID': 'tool02',
            'PredecessorID': 'tool01'
        },
        'RelationshipID'
    )
    db.set_row(
        'ToolParams',
        {
            'ParamID': 'param01',
            'ToolID': 'tool02',
            'Name': 'Input 1',
            'Value': 'fnord'
        },
        'ParamID'
    )
    db.set_row(
        'ToolParams',
        {
            'ParamID': 'param02',
            'ToolID': 'tool02',
            'Name': 'Input 2',
            'Value': 'frood'
        },
        'ParamID'
    )


def copy_config():
    OUTPUT_FOLDER.mkdir(exist_ok=True)
    shutil.copy(INPUT_MAIN, OUTPUT_MAIN)


@pytest.fixture(scope='module', autouse=True)
def setup():
    start_logging()
    build_db()


@pytest.fixture
def victim():
    return ClothoConfig(DB)

def test_import_config(victim):
    result = victim.import_config({'tools': {'Crunch Data': None}})
    assert result['tools']['Crunch Data']['ToolID'] == 'tool02'

def test_import_file(victim):
    config = victim.import_file(OUTPUT_MAIN)
    db = ClothoDB(DB)
    row = db.get_row('Resources', 'Name', 'temp folder')
    assert row['Path'] == 'temp'
    # resources = config['resources']
    # assert len(resources) == 3
    # db_resources = victim._db.get('Resources')
    # assert len(db_resources) == 3
    # assert 'drive' in list(db_resources.Name)

def test_sync_config(victim):
    victim.sync_config(INPUT_MAIN, OUTPUT_MAIN)
    victim.sync_config(INPUT_SIMPLE, OUTPUT_SIMPLE)
