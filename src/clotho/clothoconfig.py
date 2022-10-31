"""Define the ClothoConfig class."""

import os
import pathlib

import yaml

from clotho.clothodb import ClothoDB
from clotho.errors import ClothoError
from clotho.logutils import raise_error
from clotho.resource import Resource
from clotho.tool import Tool


class ClothoConfig:
    """Wraps the Clotho configuration."""

    def __init__(self, db):
        if isinstance(db, ClothoDB):
            self._db = db
        else:
            self._db = ClothoDB(db)

    def import_config(self, config=None):
        """Write a config dictionary or a config file to the database.

        :param config: A dictionary, the path to a config file, or None to use the default config
        """

        self._db.build_schema()
        if isinstance(config, dict):
            return {
                'resources': self.import_config_group(config.get('resources', {}), Resource),
                'tools': self.import_config_group(config.get('tools', {}), Tool)
            }
        self.import_file(config)

    def import_config_group(self, input_configs, config_class):
        if not input_configs:
            return {}
        output_configs = {}
        for name, config in input_configs.items():
            if config is None:
                config = {}
            config['Name'] = name
            config_obj = config_class(self._db, config)
            config_obj.commit()
            config = config_obj.config
            name = config.pop('Name')
            output_configs[name] = config
        return output_configs

    def import_file(self, config_file=None):
        if config_file:
            config_file = pathlib.Path(config_file)
        else:
            config_file = pathlib.Path(os.getcwd()) / 'ClothoConfig.yaml'

        # Get the configuration from the file.
        if not config_file.exists():
            raise_error('Config file not found: {}'.format(config_file), ClothoError)
        with open(config_file, 'r') as stream:
            config = yaml.load(stream, Loader=yaml.FullLoader)
        if config is None:
            raise_error('Config file is empty. {}'.format(config_file), ClothoError)
        return self.import_config(config)

    def sync_config(self, input_file=None, output_file=None):
        """Synchronize a config file with the config database.

        This imports the config file to the config database, then updates the config file
        with information from the database.

        If the working directory contains a ClothoConfig.yaml file, this will use it by default.

        If no output file path is provided, this will modify the input file.

        :param input_file: A YAML configuration file, or None to use ClothoConfig.yaml
        :param output_file: The path where the new version of the config file should go

        :return: The path to the output file
        """

        if not input_file:
            input_file = pathlib.Path(os.getcwd()) / 'ClothoConfig.yaml'
        if not output_file:
            output_file = input_file
        config = self.import_file(input_file)

        tool_configs = config.get('tools', {})
        tool_ids = []
        pred_ids = []
        for tool_config in tool_configs.values():
            tool_ids.append(tool_config['ToolID'])
            preds = tool_config.get('predecessors', {})
            for pred_val in preds.values():
                pred_id = pred_val.get('ToolID')
                if pred_id:
                    pred_ids.append(pred_id)
        for pred_id in set(pred_ids):
            if pred_id not in tool_ids:
                tool = Tool(self._db, id=pred_id)
                config['tools'][tool.name] = tool.config

        with open(output_file, 'w') as f:
            f.write(yaml.dump(config))
        return output_file
