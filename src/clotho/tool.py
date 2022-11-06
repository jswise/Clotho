"""Define the Tool class."""

import importlib
import logging
import uuid

import pandas as pd

from clotho.clothodb import ClothoDB
from clotho.toolparam import ToolParam
from clotho import utils
from clotho.logutils import raise_error


class ClothoToolError(Exception): pass


class Tool:
    """Represents a tool configuration."""

    def __init__(self, db, config=None, id=None, resource_shed=None) -> None:
        if isinstance(db, ClothoDB):
            self._db = db
        else:
            self._db = ClothoDB(db)

        self._config = None
        self.params = {}
        self.predecessors = None
        self.outputs = []
        self.start_time = None
        self._resource_shed = resource_shed
        self.configure(config, id)

    def commit(self):
        """Write the tool to the database."""

        # If there's no unique ID, then create one.
        if not self.id:
            self._config['ToolID'] = str(uuid.uuid1())

        # Commit the tool.
        self._db.set_row('Tools', self._config, 'ToolID')

        # Commit the parameters.
        for param in self.params.values():
            param.commit(self.id)

        # Commit the predecessors.
        pred_rows = self._db.get('ToolPredecessors', """ "ToolID" == '{}' """.format(self.id))
        db_preds = list(pred_rows.PredecessorID)
        for pred_id in list(self.predecessors.ToolID):
            if pred_id not in db_preds:
                self._db.set_row(
                    'ToolPredecessors',
                    {
                        'RelationshipID': str(uuid.uuid1()),
                        'ToolID': self.id,
                        'PredecessorID': pred_id
                    },
                    'RelationshipID'
                )

        # Commit the outputs.
        output_rows = self._db.get('ToolOutputs', """ "ToolID" == '{}' """.format(self.id))
        for i, name in enumerate(self.outputs):
            ids = list(output_rows.loc[output_rows.Name == name, 'OutputID'])
            if ids:
                id = ids[0]
            else:
                id = str(uuid.uuid1())
            self._db.set_row(
                'ToolOutputs',
                {
                    'OutputID': id,
                    'ToolID': self.id,
                    'OutputOrder': i,
                    'Name': name
                },
                'OutputID'
            )

    @property
    def config(self):
        output_config = self._config.copy()

        param_configs = {}
        for name, param in self.params.items():
            param_configs[name] = param.config
            if 'ToolID' in param_configs[name]:
                param_configs[name].pop('ToolID')
        output_config['params'] = param_configs

        preds = {}
        for _, row in self.predecessors.iterrows():
            preds[row['Name']] = {'ToolID': row['ToolID']}
        output_config['predecessors'] = preds

        output_config['outputs'] = self.outputs

        return output_config

    def configure(self, config=None, id=None):
        """Use the inputs and database to configure the tool.

        :param config: A dictionary describing the tool
        :param id: The tool's unique ID in the database
        """

        # Try getting the config by ID.
        self._config = utils.fill_config(
            self._db,
            'Tools',
            'ToolID',
            config,
            id,
            'Name'
        )

        if config is None:
            config = {}

        self.init_predecessors(config)

        # Add any extra output names from the database.
        self.outputs = utils.get_case_insensitive(config, 'outputs', [])
        output_rows = self._db.get('ToolOutputs', """ "ToolID" == '{}' """.format(self.id))
        output_rows.sort_values('OutputOrder', inplace=True)
        for db_name in output_rows.Name:
            if db_name not in self.outputs:
                self.outputs.append(db_name)

        self.init_params(config)

    def delete(self):
        """Deletes the tool and all its references from the configuration."""

        tool_id = self.id
        if not tool_id:
            logging.info("""Tool "{}" isn't in the config database.""".format(self.name))
            return
        self._db.delete_row('Tools', 'ToolID', tool_id)
        self._db.delete_row('ToolParams', 'ToolID', tool_id)
        self._db.delete_row('ToolOutputs', 'ToolID', tool_id)
        self._db.delete_row('ToolPredecessors', 'ToolID', tool_id)

    def delete_output(self, output_name):
        """Deletes an output from the configuration.

        :param output_name: The name of one of this tool's outputs.
        """

        tool_id = self.id
        if not tool_id:
            logging.info("""Tool "{}" isn't in the config database.""".format(self.name))
            return
        query = "ToolID = '{}' AND Name = '{}'".format(tool_id, output_name)
        df = self._db.get('ToolOutputs', query)
        if df.empty:
            logging.info(
                """Tool "{}" doesn't have an output called "{}".""".format(self.name, output_name)
            )
            return
        if len(df) > 1:
            logging.warning(
                """Tool "{}" had multiple outputs named "{}".""".format(self.name, output_name)
            )
        for output_id in df.OutputID.unique():
            self._db.delete_row('ToolOutputs', 'OutputID', output_id)

    def delete_param(self, param_name):
        """Deletes a parameter from the configuration.

        :param param_name: The name of one of this tool's parameters.
        """

        tool_id = self.id
        if not tool_id:
            logging.info("""Tool "{}" isn't in the config database.""".format(self.name))
            return
        query = "ToolID = '{}' AND Name = '{}'".format(tool_id, param_name)
        df = self._db.get('ToolParams', query)
        if df.empty:
            logging.info(
                """Tool "{}" doesn't have a parameter called "{}".""".format(self.name, param_name)
            )
            return
        if len(df) > 1:
            logging.warning(
                """Tool "{}" had multiple parameters named "{}".""".format(self.name, param_name)
            )
        for param_id in df.ParamID.unique():
            self._db.delete_row('ToolParams', 'ParamID', param_id)

    def end_activity(self, activity_id, succeeded=True, message=None):
        """Update an activity record with the end time.

        :param activity_id: The UUID for the activity (tool run).
        :param succeeded: Indicates whether the tool reported success
        :param message: An optional message about the activity
        """

        if message is not None:
            message = message.replace("'", '"')
        end_time = utils.get_now()
        duration = end_time - self.start_time
        self._db.update(
            'Activity',
            'ActivityID',
            activity_id,
            {
                'EndTime': utils.get_time_string(end_time),
                'Duration': utils.get_time_string(duration),
                'Succeeded': succeeded,
                'Message': message
            }
        )

    @property
    def id(self):
        return self._config.get('ToolID')

    def init_params(self, config):
        """Initialize tool parameters from a config file and the database.

        :param config: A configuration dictionary, typically from a file.
        """

        # Get the params from the config file.
        param_configs = utils.get_case_insensitive(config, 'params', {})
        param_config_ids = []
        for name, param_config in param_configs.items():
            param_config['Name'] = name
            param_config['ToolID'] = self.id
            param = ToolParam(self._db, param_config, resource_shed=self._resource_shed)
            param_id = param.config.get('ParamID')
            if param_id:
                param_config_ids.append(param_id)
            self.params[name] = param
            if not param.is_input and (param.name not in self.outputs):
                self.outputs.append(param.name)

        # Get any params from the database that weren't in the config file.
        if not self.id:
            return
        param_rows = self._db.get('ToolParams', """ "ToolID" == '{}' """.format(self.id))
        for param_id in param_rows.ParamID:
            if param_id not in param_config_ids:
                param = ToolParam(self._db, id=param_id, resource_shed=self._resource_shed)
                self.params[param.name] = param

    def init_predecessors(self, config):

        # Build a table of predecessor names & IDs based on the input config.
        pred_names = []
        pred_ids = []
        for pred_name, val in config.get('predecessors', {}).items():
            pred_names.append(pred_name)
            if val:
                pred_id = val.get('ToolID')
            else:
                pred_id = None
            if not pred_id:
                pred_tool = self._db.get_row_insensitive('Tools', 'Name', pred_name)
                if pred_tool:
                    pred_id = pred_tool['ToolID']
            pred_ids.append(pred_id)
        self.predecessors = pd.DataFrame({'Name': pred_names, 'ToolID': pred_ids})

        # Get the predecessor IDs from the database.
        if not self.id:
            return
        pred_rows = self._db.get('ToolPredecessors', """ "ToolID" == '{}' """.format(self.id))
        for pred_id in pred_rows.PredecessorID:
            if not pred_id:
                continue
            if pred_id not in pred_ids:
                tool_row = self._db.get_row('Tools', 'ToolID', pred_id)
                if tool_row is None:
                    raise_error(
                        'Predecessor tool {} not found.'.format(pred_id),
                        ClothoToolError
                    )
                pred_name = tool_row.get('Name')
                new_pred = pd.DataFrame([{'ToolID': pred_id, 'Name': pred_name}])
                self.predecessors = pd.concat([self.predecessors, new_pred])

    @property
    def name(self):
        return self._config.get('Name')

    @property
    def path(self):
        return self._config.get('Path')

    def run(self, **kwargs):
        if self.path is None:
            raise_error(
                'Path not found for tool "{}."'.format(self.name),
                ClothoToolError
            )
        pred_outputs, default_pred_name = self.run_predecessors(**kwargs)
        if pred_outputs is None:
            return

        if not self.id:
            self.commit()
        activity_id = str(uuid.uuid1())
        self.update_inputs(activity_id, pred_outputs, default_pred_name, kwargs)

        # Run the function or method that we're here for.
        mod_path, func_name = self.path.rsplit('.', 1)
        mod = importlib.import_module(mod_path)
        func = getattr(mod, func_name)
        self.start_activity(activity_id)
        try:
            output_tuple = func(**kwargs)
            if output_tuple is None:
                output_tuple = ()
            if not isinstance(output_tuple, tuple):
                output_tuple = (output_tuple,)
            output_dict = dict(zip(self.outputs, output_tuple))
        except Exception as e:
            logging.error('Tool {} failed. {}'.format(self.name, e))
            # output_dict = {'succeeded': False, 'message': str(e)}

            self.end_activity(activity_id, False, str(e))
            return

        # Associate this activity with any batches of data that we worked on.
        batch_ids = output_dict.get('batch_ids', output_dict.get('batch_id'))
        self.write_batches(batch_ids, activity_id)

        # Record the results of the activity.
        self.end_activity(activity_id, True, output_dict.get('message'))

        self.update_outputs(output_dict, activity_id, kwargs)

        return output_dict

    def run_predecessors(self, **kwargs):
        # Run the tool's predecessors.
        pred_outputs = {}
        for pred_id in self.predecessors.ToolID:
            pred = Tool(self._db, id=pred_id, resource_shed=self._resource_shed)
            result = pred.run(**kwargs)
            if result is None:
                logging.warning('"{}" failed due to "{}" failure.'.format(self.name, pred.name))
                return None, None
            pred_outputs[pred.name] = result

        default_pred_name = None
        if len(self.predecessors) == 1:
            default_pred_name = pred.name
        return pred_outputs, default_pred_name

    def start_activity(self, activity_id):
        """Create a record in the Clotho database of a tool run.

        :param activity_id: The UUID for the new activity (tool run).
        """

        self.start_time = utils.get_now()
        self._db.set_row(
            'Activity',
            {
                'ActivityID': activity_id,
                'ToolID': self.id,
                'ToolName': self.name,
                'ToolPath': self.path,
                'StartTime': utils.get_time_string(self.start_time)
            },
            'ActivityID'
        )

    def update_inputs(self, activity_id, pred_outputs, default_pred_name, kwargs):
        # Get the inputs, and save them in the I/O table.
        for name, param in self.params.items():
            if not param.is_input:
                continue
            feeder_tool_name = param.feeder_tool_name or default_pred_name
            if feeder_tool_name and param.feeder_param_name:
                try:
                    feeder_params = pred_outputs[feeder_tool_name]
                except KeyError:
                    raise_error(
                        'Feeder tool "{}" not found for tool "{}."'.format(
                            feeder_tool_name, self.name
                        ),
                        ClothoToolError
                    )
                try:
                    param.value = feeder_params[param.feeder_param_name]
                except KeyError:
                    raise_error(
                        'Feeder parameter "{}" not found for tool "{}," feeder "{}."'.format(
                            param.feeder_param_name, self.name, feeder_tool_name
                        ),
                        ClothoToolError
                    )
            val = kwargs.get(name, param.value) or param.value
            if isinstance(val, str):
                if val.lower() == 'true':
                    val = True
                elif val.lower() == 'false':
                    val = False
            kwargs[name] = val
            param.record_io(activity_id, val, self.id)

    def update_outputs(self, output_dict, activity_id, kwargs):
        # Record the outputs in the I/O table.
        for name, param in self.params.items():
            if param.is_input:
                continue
            if name in output_dict:
                param.value = output_dict[name]
            val = kwargs.get(name, param.value)
            if isinstance(val, str):
                if val.lower() == 'true':
                    val = True
                elif val.lower() == 'false':
                    val = False
            param.record_io(activity_id, val, self.id)

    def write_batches(self, batch_ids, activity_id):
        """For a given activity (tool run), record the batches that it affected.

        :param batch_ids: The unique IDs for the batches of data
        :param activity_id: The unique ID for the activity (tool run)
        """

        if batch_ids is None:
            return
        if not isinstance(batch_ids, list):
            batch_ids = [batch_ids]
        for batch_id in batch_ids:
            self._db.set_row(
                'BatchActivity',
                {
                    'RelationshipID': str(uuid.uuid1()),
                    'BatchID': batch_id,
                    'ActivityID': activity_id
                },
                'RelationshipID'
            )
