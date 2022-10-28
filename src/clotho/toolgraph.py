"""Define the ToolGraph class."""

from datetime import datetime
import logging
import pathlib
import keyring

import pandas as pd
import pydot

from clotho.clothodb import ClothoDB
from clotho.toolparam import ToolParam
from clotho.resource import Resource
from clotho.tool import Tool
from clotho.utils import extract_batch_ids


class ToolGraph:
    """Builds a visual graph of the tools."""

    def __init__(self, db, name, db_cred_list=None, max_level=-1) -> None:
        if isinstance(db, ClothoDB):
            self._db = db
        else:
            self._db = ClothoDB(db)

        self._edges = pd.DataFrame()
        self._graph = pydot.Dot(graph_type='digraph', concentrate=True)
        self.name = name
        self.init_db_creds(db_cred_list)
        self._nodes = pd.DataFrame()
        self.max_level = max_level

    def add_data(self, data_path):
        data_id = str(data_path).replace('\\\\', '/').replace('\\', '/').replace(':', '')
        if (not self._nodes.empty) and (not self._nodes[self._nodes.id == data_id].empty):
            return data_id
        data_path = pathlib.Path(data_path)
        batch_ids = extract_batch_ids(data_path, self._db_creds)
        if not batch_ids:
            return
        self.add_node(data_id, data_path.name, 'lightblue')
        for batch_id in batch_ids:
            relationships = self._db.get('BatchActivity', """ "BatchID" == '{}' """.format(batch_id))
            for activity_id in relationships.ActivityID.unique():
                activity = self._db.get_row('Activity', 'ActivityID', activity_id)
                start_time = datetime.strptime(activity['StartTime'], '%Y-%m-%d %H:%M:%S')
                end_time = datetime.strptime(activity['EndTime'], '%Y-%m-%d %H:%M:%S')
                duration = end_time - start_time
                label = '{}\n{}\n{}'.format(activity['ToolName'], activity['StartTime'], duration)
                self.add_node(activity['ActivityID'], label, 'yellowgreen', 'box')
                self.add_edge(activity['ActivityID'], data_id)
                io = self._db.get(
                    'ActivityIO',
                    """ "ActivityID" == '{}' """.format(activity_id)
                )
                io.fillna(
                    value={'IsResource': False, 'IsInput': False, 'IsRead': False},
                    inplace=True
                )
                for io_row in io.to_dict('records'):
                    if io_row['IsInput'] or io_row['IsRead']:
                        if io_row['IsResource']:
                            parent_id = self.add_data(io_row['Value'])
                            if parent_id is None:
                                continue
                            self.add_edge(parent_id, activity['ActivityID'])
                            if io_row['IsWrite']:
                                self.add_edge(activity['ActivityID'], parent_id)
                        else:
                            label = '{}:\n{}'.format(io_row['ParamName'], io_row['Value'])
                            self.add_node(io_row['IOID'], label)
                            self.add_edge(io_row['IOID'], activity['ActivityID'])
        return data_id

    def add_edge(self, src, dst, style='solid', color='black'):
        if src is None:
            if dst is None:
                logging.warning('Edge has null source and destination.')
                return False
            else:
                logging.warning('Edge has null source. Destination: {}'.format(dst))
                return False
        elif dst is None:
            logging.warning('Edge has null destination. Source: {}'.format(src))
            return False
        edge_row = pd.DataFrame(
            [
                {
                    'src': src,
                    'dst': dst,
                    'style': style,
                    'color': color
                }
            ]
        )
        self._edges = pd.concat([self._edges, edge_row])
        return True

    def add_node(self, id, label=None, fill_color='yellow', shape='ellipse', style='filled'):
        if label is None:
            label = id
        new_row = pd.DataFrame(
            [
                {
                    'id': id,
                    'label': label,
                    'style': style,
                    'fillcolor': fill_color,
                    'shape': shape
                }
            ]
        )
        old_df = self._nodes
        if not old_df.empty:
            old_df = self._nodes[self._nodes.id != id]
        self._nodes = pd.concat([old_df, new_row])

    def add_outputs(self, tool_id):
        params = self._db.get('ToolParams', """ "ToolID" == '{}' """.format(tool_id))
        param_names = list(params.Name)
        outputs = self._db.get('ToolOutputs', """ "ToolID" == '{}' """.format(tool_id))
        for _, row in outputs.iterrows():
            if row['Name'] in ['batch_id', 'batch_ids']:
                continue
            if row['Name'] in param_names:
                continue
            row['ParamID'] = row['OutputID']
            row['IsInput'] = False
            param = ToolParam(self._db, row)
            self.add_param(param)

    def add_param(self, param, level=0):
        indent = ' ' * (level + 1)
        logging.debug('{}-{}'.format(indent, param.name))
        if param.is_resource:
            # resource = Resource(self._db, {'Name': param.raw_value, 'ResourceID': param.id})
            resource = Resource(self._db, {'Name': param.raw_value})
            if not resource.id:
                resource.commit()
            node_id = resource.id
            self.add_node(node_id, param.raw_value, 'lightblue')
        else:
            node_id = param.id
            if not param.id:
                param.commit()
            self.add_node(node_id, param.name, 'yellow')

        if param.is_read:
            self.add_edge(node_id, param.tool_id)
        if param.is_write:
            self.add_edge(param.tool_id, node_id)
        if not (param.is_read or param.is_write):
            if param.is_input:
                self.add_edge(node_id, param.tool_id)
            else:
                self.add_edge(param.tool_id, node_id)

    def add_tool(self, tool, level=0):
        logging.debug('{}Adding {}.'.format(' ' * level, tool.name))
        if (not self._nodes.empty) and (self._nodes['id'] == tool.id).sum():
            return tool.id

        predecessors = {}
        if self.max_level == -1 or level < self.max_level:
            predecessor_rows = self._db.get(
                'ToolPredecessors', """ "ToolID" == '{}' """.format(tool.id)
            )
            for _, row in predecessor_rows.iterrows():
                pred_tool = Tool(self._db, id=row['PredecessorID'])
                self.add_tool(pred_tool, level + 1)
                self.add_edge(pred_tool.id, tool.id, 'dashed', 'red')
                predecessors[pred_tool.name] = pred_tool

        self.add_tool_node(tool)

        self.add_tool_params(tool, predecessors, level=level)

        self.add_outputs(tool.id)

        return tool.id

    def add_tool_node(self, tool):
        new_row = pd.DataFrame(
            [
                {
                    'id': tool.id,
                    'label': tool.name,
                    'style': 'filled',
                    'fillcolor': 'yellowgreen',
                    'shape': 'box'
                }
            ]
        )
        self._nodes = pd.concat([self._nodes, new_row])

    def add_tool_params(self, tool, predecessors=None, level=0):
        if predecessors is None:
            predecessors = {}
        for param in tool.params.values():
            if param.feeder_param_name:
                if param.feeder_tool_name:
                    feeder_tool = predecessors.get(param.feeder_tool_name)
                    if feeder_tool is None:
                        logging.warning('Feeder tool "{}" not found for {}.'.format(
                            param.feeder_tool_name,
                            tool.name
                        ))
                        continue
                else:
                    if len(predecessors) != 1:
                        logging.warning('A feeder tool is unnamed for {}.'.format(tool.name))
                        continue
                    feeder_tool = list(predecessors.values())[0]
                feeder_param = feeder_tool.params.get(param.feeder_param_name)
                if feeder_param is None:
                    logging.warning('Feeder parameter "{}" not found for {}.'.format(
                        param.feeder_param_name,
                        tool.name
                    ))
                    continue
                if feeder_param.is_resource:
                    resource = Resource(self._db, {'Name': feeder_param.raw_value})
                    if not resource.id:
                        # logging.warning('No ID found for resource {}.'.format(resource.name))
                        # continue
                        resource.commit()
                    node_id = resource.id
                else:
                    node_id = feeder_param.id
                self.add_edge(node_id, tool.id)
            else:
                self.add_param(param, level)

    def init_db_creds(self, db_cred_list):
        self._db_creds = {}
        if db_cred_list is None:
            return
        if isinstance(db_cred_list, dict):
            db_cred_list = [db_cred_list]
        for db_info in db_cred_list:
            try:
                server = db_info['server']
                database = db_info['database']
                uid = db_info['uid']
            except KeyError:
                continue
            resource_string = '{}.{}'.format(server, database)
            if 'pwd' not in db_info:
                db_info['pwd'] = keyring.get_password(resource_string, uid)
            self._db_creds[resource_string] = db_info

    def plot_data_history(self, data_path):
        self.add_data(data_path)
        self.plot_nodes()
        self.plot_edges()

    def plot_edges(self):
        self._edges.drop_duplicates(inplace=True)
        for _, row in self._edges.iterrows():
            kwargs = {}
            for column in ['src', 'dst', 'style', 'color']:
                kwargs[column] = row[column]
            edge = pydot.Edge(**kwargs)
            self._graph.add_edge(edge)

    def plot_nodes(self):
        for _, row in self._nodes.iterrows():
            kwargs = {}
            for column in ['label', 'style', 'fillcolor', 'shape']:
                kwargs[column] = row[column]
            node = pydot.Node(row['id'], **kwargs)
            self._graph.add_node(node)

    def plot_tool(self, name=None, id=None):
        if id:
            tool = Tool(self._db, id=id)
        else:
            tool = Tool(self._db, {'Name': name})
            if not tool.id:
                tool.commit()
        self.add_tool(tool)
        self.plot_nodes()
        self.plot_edges()

    def write_image(self, output_folder):
        png = str(output_folder / '{}.png'.format(self.name))
        # dot_string = self._graph.to_string()
        # print(dot_string)
        self._graph.write_png(png)  # pylint: disable=no-member
