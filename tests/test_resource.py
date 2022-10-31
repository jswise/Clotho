import os
import pathlib
import pytest
import uuid

from clotho.clothodb import ClothoDB
from clotho.logutils import start_logging
from clotho.resource import Resource

CONFIG_NAME = 'ClothoConfig.yaml'
DATA_FOLDER = pathlib.Path(__file__).parents[1] / 'data'
INPUT_CONFIG = DATA_FOLDER / 'input' / CONFIG_NAME
OUTPUT_FOLDER = DATA_FOLDER / 'output'
OUTPUT_CONFIG = OUTPUT_FOLDER / CONFIG_NAME
DB_PATH = OUTPUT_FOLDER / 'TestDB.sqlite'

GRANDPARENT_ID = 'bogus1'
PARENT_ID = 'bogus2'
RESOURCE_ID = 'bogus3'


def write_resources():
    db = ClothoDB(DB_PATH)
    db.build_schema()
    db.set_row(
        'Resources',
        {'Name': 'project folder', 'Path': 'asdf', 'ResourceID': RESOURCE_ID, 'Parent': PARENT_ID},
        'ResourceID'
    )
    db.set_row(
        'Resources',
        {'Name': 'X Drive', 'Path': 'X:', 'ResourceID': GRANDPARENT_ID},
        'ResourceID'
    )
    db.set_row(
        'Resources',
        {'Name': 'GIS', 'Path': 'GIS', 'ResourceID': PARENT_ID, 'Parent': GRANDPARENT_ID},
        'ResourceID'
    )

@pytest.fixture(scope='module', autouse=True)
def setup():
    start_logging()
    write_resources()

@pytest.fixture
def db():
    return ClothoDB(DB_PATH)


def test_init(db):
    resource = Resource(db, id=RESOURCE_ID)
    assert resource.config['Name'] == 'project folder'

    resource = Resource(db, config={'name': 'Project Folder'})
    assert resource.config['ResourceID'] == RESOURCE_ID

    resource = Resource(db)
    assert resource.config['ResourceID'] is None


def test_commit(db):
    row = db.get('Resources', """ "ResourceID" == '{}' """.format(RESOURCE_ID))
    assert row['Name'].iloc[0] == 'project folder'

    resource = Resource(db, config={'name': 'Project Folder'})
    resource.commit()

    row = db.get('Resources', """ "ResourceID" == '{}' """.format(RESOURCE_ID))
    assert row['Name'].iloc[0] == 'Project Folder'


def test_configure():
    victim = Resource(DB_PATH)
    victim.configure({'Name': 'TestResource'})
    assert victim.name == 'TestResource'


def test_id():
    victim = Resource(DB_PATH, {'Name': 'TestResource2'})
    # assert victim.id is None
    victim.commit()
    assert victim.id


def test_init_parent(db):
    victim = Resource(DB_PATH, id=RESOURCE_ID)
    assert victim.config['Parent'] == PARENT_ID


def test_name(db):
    victim = Resource(DB_PATH, id=RESOURCE_ID)
    assert victim.name.lower() == 'project folder'


def test_path(db):
    resource = Resource(db, config={'name': 'project folder'})
    assert resource.path == 'X:/GIS/asdf'
    resource.path = 'qwer'
    assert resource.path == 'X:/GIS/qwer'
