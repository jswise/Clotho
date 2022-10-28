"""Define the ToolParam class."""

import uuid

from clotho.clothodb import ClothoDB
from clotho.resource import Resource
from clotho.utils import fill_config
from clotho.utils import get_case_insensitive
from clotho.logutils import raise_error

class ToolParam:
    """Represents a tool parameter configuration."""

    def __init__(self, db, config=None, id=None, resource_shed=None) -> None:
        if isinstance(db, ClothoDB):
            self._db = db
        else:
            self._db = ClothoDB(db)
        
        self.config = None
        self.name = None
        self._resource_shed = resource_shed

        self.configure(config, id)

    def commit(self, tool_id):
        """Write the parameter to the database.

        :param tool_id: The unique ID for the tool that uses this parameter
        """

        self.config['ToolID'] = tool_id

        # If there's no unique ID, then create one.
        if not self.config.get('ParamID'):
            self.config['ParamID'] = str(uuid.uuid1())

        # Create or update the table row.
        output_config = self.config.copy()
        output_config['Name'] = self.name
        self._db.set_row('ToolParams', output_config, 'ParamID')

    def configure(self, config=None, id=None):
        """Use the inputs and database to configure the parameter.
        
        :param config: A dictionary describing the parameter
        :param id: The parameter's unique ID in the database
        """

        # Try getting the config by ID.
        self.config = fill_config(
            self._db,
            'ToolParams',
            'ParamID',
            config,
            id
        )

        # If that didn't work, try getting it by tool ID & param name.
        # Note that different tools can have the same param names.
        if not self.config['ParamID'] and self.config['ToolID']:
            big_df = self._db.get('ToolParams')
            name_df = big_df[big_df['Name'].str.lower() == self.config['Name'].lower()]
            df = name_df[name_df.ToolID == self.config['ToolID']]
            if not df.empty:
                row = df.to_dict('records')[0]
                for key, db_val in row.items():
                    self.config[key] = get_case_insensitive(config, key, db_val)

        self.name = self.config.pop('Name')
        if self.config.get('IsInput') is None:
            self.config['IsInput'] = True

    @property
    def feeder_param_name(self):
        return self.config.get('FeederParamName')

    @property
    def feeder_tool_name(self):
        return self.config.get('FeederToolName')

    def get_resource(self):
        """If this parameter represents a resource, then get the resource.

        :return: A resource, if it exists
        """

        if not self.config.get('IsResource'):
            return

        if self._resource_shed:
            return self._resource_shed.get(self.config.get('Value'))
        return Resource(self._db, {'Name': self.config.get('Value')})

    @property
    def id(self):
        return self.config.get('ParamID')

    @property
    def is_input(self):
        return self.config.get('IsInput')

    @property
    def is_read(self):
        return self.config.get('IsRead')

    @property
    def is_resource(self):
        return self.config.get('IsResource')

    @property
    def is_write(self):
        return self.config.get('IsWrite')

    @property
    def raw_value(self):
        return self.config.get('Value')

    def record_io(self, activity_id, value, tool_id=None):
        """Write metadata about the input to this parameter during activity.
        
        :param activity_id: The unique ID for the current activity (tool run)
        :param value: The argument to the parameter
        :param tool_id: The unique ID if the tool that uses this parameter
        """

        tool_id = tool_id or self.config.get('ToolID')
        if tool_id is None:
            raise_error('No tool ID set for param {}.'.format(self.name))
        if not self.id:
            self.commit(tool_id)
        self._db.set_row(
            'ActivityIO',
            {
                'IOID': str(uuid.uuid1()),
                'ActivityID': activity_id,
                'ParamID': self.id,
                'ParamName': self.name,
                'Value': value,
                # 'Feeder': self.config.get('Feeder'),
                'IsResource': self.config.get('IsResource', False),
                'IsInput': self.is_input,
                'IsRead': self.is_read,
                'IsWrite': self.is_write
            },
            'IOID'
        )

    @property
    def tool_id(self):
        return self.config.get('ToolID')

    @property
    def value(self):
        resource = self.get_resource()
        if resource:
            return resource.path
        return self.config.get('Value')
    
    @value.setter
    def value(self, val):
        resource = self.get_resource()
        if resource:
            resource.path = val
        else:
            self.config['Value'] = val
