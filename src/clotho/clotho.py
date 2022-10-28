"""Define the Clotho class."""

import logging

from clotho.clothoconfig import ClothoConfig
from clotho.clothodb import ClothoDB
from clotho.resourceshed import ResourceShed
from clotho.tool import Tool


class Clotho:
    """Orchestrates Clotho's processes."""

    def __init__(self, source) -> None:
        """Instantiate a Clotho object to use for all Clotho interactions.

        The source represents the database where Clotho will store configurations and metadata.
        It can be a path to a SQLite database file (which doesn't need to exist yet),
        a path to a folder for storing CSV files (which also doesn't need to exist yet),
        or a dictionary containing "server" (a SQL server machine), "database" (the name of an
        existing SQL Server database), "uid" (a user ID), "pwd" (a password), and optionally
        the name of a driver (the default is "SQL+Server").

        :param source: The location or connection information for a database
        """

        self._db = ClothoDB(source)
        self._resource_shed = ResourceShed(self._db)

    def delete_tool(self, tool_name=None, tool_id=None):
        """Delete a Clotho tool. Requires either a name or an ID.

        :param tool_name: The unique name of a configured tool
        :param tool_id: The unique ID of a configured tool
        """

        tool = Tool(self._db, {'Name': tool_name}, tool_id, self._resource_shed)
        tool.delete()

    def delete_tool_output(self, output_name, tool_name=None, tool_id=None):
        """Delete one of a tool's outputs. Requires either a tool name or a tool ID.

        :param output_name: The name of one of this tool's outputs.
        :param tool_name: The unique name of a configured tool
        :param tool_id: The unique ID of a configured tool
        """

        tool = Tool(self._db, {'Name': tool_name}, tool_id, self._resource_shed)
        tool.delete_output(output_name)

    def delete_tool_param(self, param_name, tool_name=None, tool_id=None):
        """Delete one of a tool's parameters. Requires either a tool name or a tool ID.

        :param param_name: The name of one of this tool's parameters.
        :param tool_name: The unique name of a configured tool
        :param tool_id: The unique ID of a configured tool
        """

        tool = Tool(self._db, {'Name': tool_name}, tool_id, self._resource_shed)
        tool.delete_param(param_name)

    def build_schema(self):
        """Create the Clotho schema in the database."""

        self._db.build_schema()

    def import_config(self, config=None):
        """Write a config dictionary or a config file to the database.

        :param config: A dictionary, the path to a config file, or None to use the default config
        """

        clotho_config = ClothoConfig(self._db)
        clotho_config.import_config(config)

    def run_tool(self, tool_name=None, tool_id=None, **kwargs):
        """Run a Clotho tool. Requires either a name or an ID.

        :param tool_name: The unique name of a configured tool
        :param tool_id: The unique ID of a configured tool
        :param kwargs: Any arguments to send to the tools

        :return: Whatever output the tool returns
        """

        tool = Tool(self._db, {'Name': tool_name}, tool_id, self._resource_shed)
        return tool.run(**kwargs)

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

        clotho_config = ClothoConfig(self._db)
        return clotho_config.sync_config(input_file, output_file)
