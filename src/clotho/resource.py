"""Define the Resource class."""

import logging
import uuid

from clotho.clothodb import ClothoDB
from clotho.utils import fill_config


class Resource:
    """Represents a source of data."""

    def __init__(self, db, config=None, id=None, resource_shed=None) -> None:
        if isinstance(db, ClothoDB):
            self._db = db
        else:
            self._db = ClothoDB(db)

        self.config = None
        self._parent_row = None
        self._resource_shed = resource_shed

        self.configure(config, id)

    def commit(self):
        """Write the resource to the database."""

        # If there's no unique ID, then create one.
        if not self.config.get('ResourceID'):
            self.config['ResourceID'] = str(uuid.uuid1())

        # Create or update the table row.
        self._db.set_row('Resources', self.config, 'ResourceID')

    def configure(self, config=None, id=None):
        """Use the inputs and database to configure the resource.

        :param config: A dictionary describing the resource
        :param id: The resource's unique ID in the database
        """

        # Try getting the config by ID.
        filled_config = fill_config(
            self._db,
            'Resources',
            'ResourceID',
            config,
            id
        )
        if filled_config.get('ResourceID'):
            self.config = filled_config
            self.init_parent()
            return

        # If that didn't work, try using the name.
        self.config = fill_config(
            self._db,
            'Resources',
            'Name',
            config
        )

        self.init_parent()

    @property
    def id(self):
        return self.config.get('ResourceID')

    def init_parent(self):
        """Find the unique ID for this resource's parent resource."""

        # See if this resource has a parent.
        parent_val = self.config.get('Parent')
        if not parent_val:
            return

        # See if the value provided is an existing unique ID or name.
        self._parent_row = self._db.get_row('Resources', 'ResourceID', parent_val)
        if not self._parent_row:
            self._parent_row = self._db.get_row_insensitive('Resources', 'Name', parent_val)
        if not self._parent_row:
            logging.warning('Parent resource "{}" not found.'.format(parent_val))
            return

        # Update the configuration with the parent ID (possibly unchanged).
        self.config['Parent'] = self._parent_row['ResourceID']

    @property
    def name(self):
        return self.config['Name']

    @property
    def path(self):
        """Get the path to this resource, including parent resources.

        :return: The complete path to the resource
        """

        path = self.config.get('Path')
        if not self._parent_row:
            return path

        if self._resource_shed:
            parent = self._resource_shed.get(self._parent_row['Name'])
        else:
            parent = Resource(self._db, id=self._parent_row['ResourceID'])
        if parent is None or parent.path is None or path is None:
            return

        return parent.path + '/' + str(path)

    @path.setter
    def path(self, new_path):
        self.config['Path'] = new_path
