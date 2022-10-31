import pathlib
import shutil
import uuid

import pytest

from clotho.clothodb import ClothoDB
from clotho.logutils import start_logging
from clotho.tool import Tool

DATA_FOLDER = pathlib.Path(__file__).parents[1] / 'data'
INPUT_FOLDER = DATA_FOLDER / 'input'
OUTPUT_FOLDER = DATA_FOLDER / 'output'
DB_PATH = OUTPUT_FOLDER / 'TestDB.sqlite'

# INPUT_CLOTHO_DB = INPUT_FOLDER / 'SkiWX' / 'ClothoDB'
# OUTPUT_CLOTHO_DB = OUTPUT_FOLDER / 'SkiWX' / 'ClothoDB'


@pytest.fixture
def victim():
    tool_config = {
        'Name': 'TestTool',
        'Path': 'clotho.utils.quote_val',
        'Params': {
            'val': {
                'value': 'asdf'
            }
        }
    }
    return Tool(DB_PATH, tool_config)


@pytest.fixture(scope='module', autouse=True)
def setup():
    start_logging()

    # OUTPUT_CLOTHO_DB.mkdir(parents=True, exist_ok=True)
    # shutil.copytree(INPUT_CLOTHO_DB, OUTPUT_CLOTHO_DB, dirs_exist_ok=True)

    if DB_PATH.exists():
        DB_PATH.unlink()
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    db = ClothoDB(DB_PATH)
    db.build_schema()


def test_commit(victim):
    assert victim.id is None
    victim.commit()
    assert victim.id


def test_config(victim):
    assert victim.config['Name'] == 'TestTool'


def test_configure():
    victim = Tool(DB_PATH)
    victim.configure({'Name': 'TestTool'})
    assert victim.name == 'TestTool'


def test_end_activity(victim):
    activity_id = str(uuid.uuid1())
    victim.start_activity(activity_id)
    victim.end_activity(activity_id)
    db = ClothoDB(DB_PATH)
    row = db.get_row('Activity', 'ActivityID', activity_id)
    assert row['EndTime']


def test_id():
    victim = Tool(DB_PATH, {'Name': 'TestTool2'})
    assert victim.id is None
    victim.commit()
    assert victim.id


def test_init_params(victim):
    victim.init_params({'params': {'val': {'value': 'asdf'}}})
    victim.commit()
    victim = Tool(DB_PATH, {'Name': 'TestTool'})
    assert 'val' in victim.params


def test_init_predecessors(victim):
    victim.init_predecessors({'predecessors': {'Pred 1': None}})
    assert not victim.predecessors.empty


def test_name(victim):
    assert victim.name == 'TestTool'


def test_path(victim):
    assert victim.path == 'clotho.utils.quote_val'


def test_run(victim):
    victim.run()


def test_start_activity(victim):
    activity_id = str(uuid.uuid1())
    victim.start_activity(activity_id)
    db = ClothoDB(DB_PATH)
    row = db.get_row('Activity', 'ActivityID', activity_id)
    assert row['ToolName'] == 'TestTool'


def test_write_batches(victim):
    victim.write_batches('Batch1', 'Activity2')
    victim.write_batches(['Batch2', 'Batch3'], 'Activity3')
    db = ClothoDB(DB_PATH)
    assert db.get_row('BatchActivity', 'BatchID', 'Batch1')['ActivityID'] == 'Activity2'
    assert db.get_row('BatchActivity', 'BatchID', 'Batch2')['ActivityID'] == 'Activity3'
    assert db.get_row('BatchActivity', 'BatchID', 'Batch3')['ActivityID'] == 'Activity3'
