'''
File containing functions for in-depth exploration of queries performed on a PostGreSQL database.
'''
import numpy as np
import psycopg
import plotly.graph_objects as go
from igraph import Graph
from pydantic import BaseModel, ConfigDict, UUID4
from typing import Dict, Any
from uuid import uuid4
import re
import math
import traceback

BLK_SIZE = 8192
MAX_OUTPUT_LENGTH = 50

PG_CONDS = ['Filter', 'Join Filter', 'Hash Cond', 'Index Cond', 'Merge Cond',
            'Recheck Cond', 'Group Key', 'Sort Key', 'Presorted Key', 'Cache Key']

PG_TYPES = [
    'numeric', 'decimal', 'real', 'double precision', 'float4', 'float8',
    'bigint', 'smallint', 'integer', 'int2', 'int4', 'int8',
    'bigserial', 'smallserial', 'serial2', 'serial4', 'serial8',
    'bit varying', 'varbit', 'bytea', 'boolean', 'bool',
    'bpchar', 'character', 'character varying', 'varchar', 'text', 'tsquery', 'tsvector',
    'box', 'circle', 'line', 'lseg', 'point', 'polygon', 'path',
    'pg_lsn', 'pg_snapshot', 'txid_snapshot', 'cidr', 'inet', 'macaddr8', 'macaddr',
    'date', 'interval', 'time without time zone', 'time with time zone', 'timetz',
    'timestamp without time zone', 'timestamp with time zone', 'timestamptz', 'timestamp',
    'money', 'uuid', 'xml', 'jsonb', 'json', 'time', 'serial', 'bit', 'int', 'char'
]

# Constants
# CPU cost constants
DEFAULT_CPU_TUPLE_COST = 0.01  # Cost of processing one tuple
DEFAULT_CPU_INDEX_TUPLE_COST = 0.005  # Cost of processing one index tuple
DEFAULT_CPU_OPERATOR_COST = 0.0025  # Cost of executing one operator or function

# Parallel query constants
# Cost of passing one tuple from worker to leader
DEFAULT_PARALLEL_TUPLE_COST = 0.1
# Cost of setting up shared memory for parallelism
DEFAULT_PARALLEL_SETUP_COST = 1000.0

# Disk access cost constants
DEFAULT_SEQ_PAGE_COST = 1.0  # Cost of fetching a sequential page from disk
# Cost of fetching a non-sequential (random) page from disk
DEFAULT_RANDOM_PAGE_COST = 4.0

# Other constants
APPEND_CPU_COST_MULTIPLIER = 0.1  # CPU cost multiplier for MergeAppend operations
LOG2 = math.log2  # Natural logarithm base 2 function

# Assign the constants to variables
cpu_tuple_cost = DEFAULT_CPU_TUPLE_COST
cpu_index_tuple_cost = DEFAULT_CPU_INDEX_TUPLE_COST
cpu_operator_cost = DEFAULT_CPU_OPERATOR_COST
parallel_tuple_cost = DEFAULT_PARALLEL_TUPLE_COST
parallel_setup_cost = DEFAULT_PARALLEL_SETUP_COST
seq_page_cost = DEFAULT_SEQ_PAGE_COST
random_page_cost = DEFAULT_RANDOM_PAGE_COST


class QEPNode(BaseModel):
    node_id: UUID4
    node_type: str

    planner_cost: float = None  # Cost estimated by planner for the node
    our_cost: float = -1  # Our estimated cost for the node

    size: int = None  # The size of the intermediate relation, in bytes
    our_size: int = -1  # Our estimated size of the intermediate relation, in bytes

    shared_blocks_hit: int = None
    shared_blocks_read: int = None
    execution_time: float = None

    # Any other properties that aren't explicitly captured here
    properties: Dict[str, Any] = {}
    left_child: 'QEPNode' = None
    right_child: 'QEPNode' = None

    def show(self, cost, size, level=0):
        if cost and size:
            show_str = self.node_type + "(Our cost: " + str(self.our_cost) + ", Our size: " + str(
                self.our_size) + ", Planner cost: " + str(self.planner_cost) + ", Size: " + str(self.size) + ")\n"
        if cost and not size:
            show_str = self.node_type + \
                "(Our cost: " + str(self.our_cost) + \
                ", Planner cost: " + str(self.planner_cost) + ")\n"
        if not cost and size:
            show_str = self.node_type + \
                "(Our size: " + str(self.our_size) + \
                ", Size: " + str(self.size) + ")\n"
        if not cost and not size:
            show_str = self.node_type + "\n"
        ret = "\t" * level + show_str
        if self.left_child:
            ret += self.left_child.show(cost, size, level + 1)
        if self.right_child:
            ret += self.right_child.show(cost, size, level + 1)
        return ret


    def update_sizes(self, num_distinct_dict):
        if self.left_child:
            self.left_child.update_sizes(num_distinct_dict)
        if self.right_child:
            self.right_child.update_sizes(num_distinct_dict)
        if self.node_type == 'Hash':
            self.our_size = get_previous_node_size(self)
        if self.node_type == 'Gather Merge':
            self.our_size = get_previous_node_size(self) * 2
        if self.node_type == 'Gather':
            self.our_size = get_previous_node_size(self) * 2
        if "Scan" in self.node_type:
            self.our_size = self.size
        if self.node_type == "Aggregate":
            num_group_keys = len(self.properties.get('Group Key', [1]))
            self.our_size = self.properties['Plan Width'] * num_group_keys

        if self.node_type == "Sort":
            self.our_size = get_previous_node_size(self)

        if "Join" in self.node_type or self.node_type == "Nested Loop":
            print(self.node_type)
            inner_size = self.left_child.properties['Actual Rows']
            outer_size = self.right_child.properties['Actual Rows']
            expected_size = inner_size * outer_size
            denominator = 1

            join_type = self.node_type


            if join_type == "Hash Join":
                
                size = inner_size * outer_size
                
                
                join_cond = self.properties['Hash Cond'].replace(
                    "(", "").replace(")", "")

                # FOR ONE CONDITION
                operator_list = ["=", "<", ">", "<=", ">="]
                for operator in operator_list:
                    if operator in join_cond:
                        break
                columns = join_cond.split(operator)
                tables = [column.split(".")[0] for column in columns]

                inner_table_alias = tables[0].strip()
                outer_table_alias = tables[1].strip()

                # need to find the actual table names,
                # they are given as children of the join node, keep searching until you find the table with the alias
                alias_to_name_map = self._get_all_alias_names(self)
                inner_table = alias_to_name_map[inner_table_alias]
                outer_table = alias_to_name_map[outer_table_alias]

                table_columns = [column.split(".")[1] for column in columns]

                inner_table_col = table_columns[0].strip()
                outer_table_col = table_columns[1].strip()
                inner_num_distinct = num_distinct_dict[inner_table][inner_table_col]
                outer_num_distinct = num_distinct_dict[outer_table][outer_table_col]
                denominator *= max(inner_num_distinct, outer_num_distinct)

                self.our_size = expected_size / (denominator) * self.left_child.properties['Plan Width']
            elif join_type == "Nested Loop":
                size = inner_size * outer_size

                inner_width = self.left_child.properties['Plan Width']
                outer_width = self.right_child.properties['Plan Width']

                expected_size = size * (inner_width + outer_width)
                self.our_size = expected_size
            else:
                record_size = self.left_child.properties['Plan Width'] + \
                    self.right_child.properties['Plan Width']
                rows = 0
                if self.properties['Parent Relationship'] == "Outer":
                    rows = max(
                        self.left_child.properties['Plan Rows'], self.right_child.properties['Plan Rows'])
                size = record_size * rows
                self.our_size = size


        if self.node_type == "Limit":
            limit = self.properties['Plan Rows']
            size = self.properties['Plan Width']
            self.our_size = limit * size
            print('Limit', limit, size, self.our_size)

    def _get_all_alias_names(self, node, alias_to_name_map={}):
        if "Scan" in node.node_type:
            alias_to_name_map[node.properties['Alias']
                              ] = node.properties['Relation Name']
        if node.left_child:
            self._get_all_alias_names(node.left_child, alias_to_name_map)
        if node.right_child:
            self._get_all_alias_names(node.right_child, alias_to_name_map)
        return alias_to_name_map


def get_previous_node_size(node):
    if node.left_child is not None:
        return node.left_child.our_size
    if node.right_child is not None:
        return node.right_child.our_size
    else:
        return node.size

class QEP(BaseModel):
    root: QEPNode
    planning_time: float
    execution_time: float = None
    average_node_execution_time: float = None  # Average time spent in each node
    shared_blocks_hit: int = None  # If the plan was cached
    shared_blocks_read: int = None
    
    def show(self, cost=True, size=True):
        return self.root.show(cost, size)
    

class PGServer(BaseModel):
    
    model_config = ConfigDict(arbitrary_types_allowed=True)

    conn: psycopg.Connection = None
    current_query: str = None
    current_qep: QEP = None
    qep_graph: Graph = None

    tuple_count_dict: Dict[str, int] = {}
    num_distinct_dict: Dict[str, Dict[str, int]] = {}
    row_width_dict: Dict[str, int] = {}

    def connect(self, host: str, port: str, database: str, user: str, password: str) -> str:
        """
        Connect to the specified database using the provided credentials
        """
        try:
            conn_string = f'host={host} port={port} dbname={database} user={user} password={password}'
            print("Connecting...")
            self.conn = psycopg.connect(conn_string)
        
            
            print('Connected to database')
            return ('Connected')
        except Exception as e:
            print(traceback.format_exc())
            print(f'Error: {e}')
            error_msg = f'Error: {str(e).rpartition(":")[2].strip(" ").capitalize()}\nPlease check your connection details and try again.'
            return error_msg

    def disconnect(self) -> str:
        """
        Close the connection to the database
        """
        try:
            self.conn.close()
            print('Disconnected from database')
        except Exception as e:
            print(traceback.format_exc())
            print(f'Error: {e}')

        return ('Not connected')

    def execute_query(self) -> tuple[list[str], list[Any]] | str:
        """
        Execute the query to obtain the query results (records).
        """
        try:
            if self.current_query is None:
                self.current_qep = None
                return 'Error: No query provided. Please enter a query and try again.'

            with self.conn.cursor() as cur:
                cur.execute(self.current_query)
                data = cur.fetchmany(MAX_OUTPUT_LENGTH)
                headers = [desc[0] for desc in cur.description]
                return headers, data

        except Exception as e:
            print(traceback.format_exc())
            print(f'Error: {e}')
            with self.conn.cursor() as cur:
                cur.execute("ROLLBACK;")
            return f'Error:\n{e}\nPlease restart the database connection and try again.'

    def _build_qep_node(self, qep_node_dict: dict[str, Any]) -> QEPNode:
        """
        Build a QEPNode from the provided dictionary of qep node properties obtained by running EXPLAIN
        """
        node_type = qep_node_dict['Node Type']
        shared_blocks_hit = qep_node_dict.get('Shared Hit Blocks', 0)
        shared_blocks_read = qep_node_dict.get('Shared Read Blocks', 0)
        size = qep_node_dict.get("Plan Rows") * qep_node_dict.get("Plan Width")
        exclude = ['Node Type', 'Shared Hit Blocks',
                   'Shared Read Blocks', 'Plans'
                   ]
        properties = {k: qep_node_dict[k] for k in set(
            list(qep_node_dict.keys())) - set(exclude)}  # Get the remaining properties
        children = qep_node_dict.get('Plans', [])
        node = QEPNode(
            node_id=uuid4(),
            node_type=node_type,
            shared_blocks_hit=0 if not shared_blocks_hit else shared_blocks_hit,
            shared_blocks_read= 0 if not shared_blocks_read else shared_blocks_read,
            size=size,
            properties=properties
        )
        num_children = 1

        if len(children) == 1:
            leftNode, num_left_children = self._build_qep_node(children[0])
            node.left_child = leftNode
            num_children += num_left_children
        elif len(children) == 2:
            leftNode, num_left_children = self._build_qep_node(children[0])
            node.left_child = leftNode
            rightNode, num_right_children = self._build_qep_node(children[1])
            node.right_child = rightNode
            num_children += num_left_children + num_right_children
        elif len(children) > 2:
            raise RuntimeError(
                "Unable to parse QEP Tree.\nMore than 2 child nodes were found.\nThe current implementation does not support non-binary QEP trees.")
        return node, num_children

    def get_qep(self, query: str, options: list[str]) -> QEP | str:
        """
        Run EXPLAIN with the specified options to obtain QEP.
        This transaction will rollback to avoid side effects from affecting the database.
        """
        try:
            if query is None or query == '':
                self.current_query = None
                return 'Error: No query provided. Please enter a query and try again.'

            self.current_query = query
            with self.conn.cursor() as cur:
                options = f'({", ".join(options)}, FORMAT JSON)'
                cur.execute("BEGIN;")
                cur.execute(
                    f'EXPLAIN {options} {self.current_query}{";" if not self.current_query.endswith(";") else ""}')

                # Extract information from the response
                qep = cur.fetchone()[0][0]
                print('QEP obtained')
                qep_planning_time = qep.get("Planning Time", None)
                qep_execution_time = qep.get("Execution Time", None)
                qep_shared_blocks_hit = qep.get(
                    "Planning", {}).get("Shared Hit Blocks", None)
                qep_shared_blocks_read = qep.get(
                    "Planning", {}).get("Shared Read Blocks", None)


                # Recursively build the QEP tree, returning the root node
                print('Building QEP tree...')
                qep_root_node, total_number_of_nodes = self._build_qep_node(
                    qep['Plan'])
                print('QEP tree built')

                total_node_execution_time = qep_root_node.properties['Actual Total Time']
                average_node_execution_time = (
                    total_node_execution_time / total_number_of_nodes) if qep_execution_time is not None else None
                # Add execution time to each node
                self._check_time_recursive(qep_root_node)

                
                # Add planner costs to each node

                self._get_node_cost(qep_root_node)
                
                if len(self.row_width_dict.keys()) ==0:
                    self.row_width_dict = self.get_average_row_widths()
                
                self._estimate_size(qep_root_node)
                
                if len(self.num_distinct_dict.keys()) == 0 and len(self.tuple_count_dict.keys()) == 0:
                    self.num_distinct_dict = self.get_num_distinct_values()
                    self.tuple_count_dict = self.get_tuple_counts()
                
                self._estimate_cost(qep_root_node)
                print('Costs estimated')
                print(qep)
                
                qep = QEP(root=qep_root_node,
                            planning_time=qep_planning_time,
                            execution_time=qep_execution_time,
                            average_node_execution_time=average_node_execution_time,
                            shared_blocks_hit=0 if not qep_shared_blocks_hit else qep_shared_blocks_hit,
                            shared_blocks_read= 0 if not qep_shared_blocks_read else qep_shared_blocks_read
                            )

                
                self.current_qep = qep
                cur.execute("ROLLBACK;")
 
                return qep

        except Exception as e:
            print(traceback.format_exc())
            print(f'Error in get_qep function: {e}')
            self.current_qep = None
            with self.conn.cursor() as cur:
                cur.execute("ROLLBACK;")
            return f'Error:\n{e}\nPlease restart the database connection and try again.'


    def _check_time_recursive(self, qep_node: QEPNode) -> float:
        """
        Recursively computes and checks for the execution time of each individual node
        """
        if qep_node == None:
            return 0

        time_left_child = self._check_time_recursive(qep_node.left_child)
        time_right_child = self._check_time_recursive(qep_node.right_child)

        if "Workers" in qep_node.properties:
            node_execution_time = qep_node.properties["Actual Total Time"] - \
                time_left_child - time_right_child
            qep_node.execution_time = node_execution_time
            return qep_node.properties["Actual Total Time"]
        else:
            time_qep_node = qep_node.properties["Actual Loops"] * \
                qep_node.properties["Actual Total Time"] - \
                time_left_child - time_right_child
            qep_node.execution_time = time_qep_node
            return qep_node.properties["Actual Loops"] * qep_node.properties["Actual Total Time"]

    def _get_node_cost(self, qep_node: QEPNode) -> float:
        """
        Compute the planner's estimate for the cost of each node
        """
        if qep_node == None:
            return 0

        cost_left_child = self._get_node_cost(qep_node.left_child)
        cost_right_child = self._get_node_cost(qep_node.right_child)

        if qep_node.node_type == 'Limit':
            current_node_cost = qep_node.properties['Total Cost']
        else:
            current_node_cost = qep_node.properties["Total Cost"] - \
                cost_left_child - cost_right_child

        qep_node.planner_cost = current_node_cost
        return qep_node.properties["Total Cost"]

    def _estimate_size(self, qep_root_node: QEPNode) -> int:
        qep_root_node.update_sizes(self.num_distinct_dict)

    def _estimate_cost(self, qep_root_node) -> None:
        if qep_root_node is None:
            return
        if qep_root_node.left_child is not None:
            self._estimate_cost(qep_root_node.left_child)
        if qep_root_node.right_child is not None:
            self._estimate_cost(qep_root_node.right_child)
        self._estimate_node_cost(qep_root_node)
        self._estimate_size(qep_root_node)

    def _estimate_node_cost(self, qep_node: QEPNode) -> None:
        if qep_node.node_type == 'Sort':
            self._estimate_sort(qep_node)
        if qep_node.node_type == 'Hash':
            self._estimate_hash_cost(qep_node)
        if qep_node.node_type == 'Aggregate':
            self._estimate_agg_cost(qep_node)

        if qep_node.node_type == 'Gather':
            self._estimate_gather(qep_node)
        if qep_node.node_type == 'Gather Merge':
            self._estimate_gather_merge(qep_node)

        if qep_node.node_type == 'Limit':
            self._estimate_limit_cost(qep_node)

        if qep_node.node_type == 'Seq Scan':
            self._estimate_seq_scan_cost(qep_node)
        if qep_node.node_type == 'Index Scan' or qep_node.node_type == 'Index Only Scan':
            self._estimate_indexScan_cost(qep_node)
        if qep_node.node_type == 'Bitmap Heap Scan' or qep_node.node_type == 'Bitmap Index Scan':
            self._estimate_bitmapScan_cost(qep_node)

        if qep_node.node_type == 'Hash Join':
            self._estimate_hashJoin_cost(qep_node)
        if qep_node.node_type == 'Merge Join':
            self._estimate_mergeJoin_cost(qep_node)
        if qep_node.node_type == 'Nested Loop':
            self._estimate_nestedLoop_cost(qep_node)

    def _estimate_hash_cost(self, qep_node: QEPNode) -> None:
        qep_node.our_cost = 0.0

    def _estimate_seq_scan_cost(self, qep_node: QEPNode) -> None:
        relation_name = qep_node.properties['Relation Name']
        num_tuples = self.tuple_count_dict[relation_name]
        row_width = self.row_width_dict[relation_name]
        num_pages = math.ceil(num_tuples * row_width / BLK_SIZE)

        disk_cost = seq_page_cost * num_pages
        cpu_cost = cpu_tuple_cost * num_tuples

        if 'Filter' in qep_node.properties:
            num_filters = qep_node.properties['Filter'].count(
                ' AND ') + qep_node.properties['Filter'].count(' OR ') + 1
            conditions = qep_node.properties['Filter'].split(
                ' AND ') + qep_node.properties['Filter'].split(' OR ')
            for cond in conditions:
                if ' > ' in cond or ' < ' in cond or ' >= ' in cond or ' <= ' in cond:
                    num_filters += 1
                    num_tuples = num_tuples / 3
                else:
                    num_filters += 1
                    attr_name = cond.split(' ')[0].replace('(', '').replace(')', '').split('.')[1]
                    # find the column name that is being indexed from condition
                    print('attribute_name:', attr_name)
                    num_tuples = num_tuples / \
                        self.num_distinct_dict[relation_name][attr_name]

            cpu_cost += num_filters * cpu_operator_cost * num_tuples

        DISCOUNT_FACTOR = 0.61

        if disk_cost < 1.5:
            disk_cost = disk_cost * 10

        qep_node.our_cost = disk_cost + cpu_cost * DISCOUNT_FACTOR

    def _estimate_agg_cost(self, qep_node: QEPNode) -> None:
        rows_processed = qep_node.left_child.properties['Plan Rows']
        rows_returned = qep_node.properties['Plan Rows']
        qep_node.our_cost = (rows_processed*cpu_operator_cost + rows_returned*cpu_tuple_cost) * 2


    def _estimate_gather(self, qep_node: QEPNode) -> None:
        num_tuples = qep_node.left_child.our_size

        cost = parallel_setup_cost + parallel_tuple_cost * num_tuples

        qep_node.our_cost = cost

    def _estimate_gather_merge(self, qep_node: QEPNode) -> None:
            num_workers = qep_node.properties.get('Workers Planned', 2)
            N = num_workers + 1
            logN = LOG2(N)

            tuples = qep_node.properties['Plan Rows'] 
            comparison_cost = 2  * cpu_operator_cost
            startup_cost = comparison_cost * N * logN
            run_cost = tuples * comparison_cost * logN
            run_cost+= cpu_operator_cost * tuples
            
            startup_cost+= parallel_setup_cost
            run_cost+= tuples * parallel_tuple_cost*1.05 #1,05 is a 5% charge for blocking workers out of memory given by PostgreSQL
            cost = startup_cost + run_cost
            qep_node.our_cost = cost

    def _estimate_sort(self, qep_node: QEPNode, rightChild=False):
        if not rightChild:
            num_tuples = qep_node.left_child.our_size
        else:
            num_tuples = qep_node.our_size
        comparison_cost = 2 * cpu_operator_cost
        tape_buffer_overhead = BLK_SIZE
        merge_buffer_size = BLK_SIZE*32
        sort_work_mem = 4*1024*1024
        minorder = 6
        maxorder = 500
        size = num_tuples
        startup_cost = 0
        tuples= num_tuples/qep_node.properties['Plan Width']
        if size > sort_work_mem:
            npages = math.ceil(size/BLK_SIZE)
            nruns = size/sort_work_mem
            morder = sort_work_mem/(2*tape_buffer_overhead + merge_buffer_size)
            morder = max(morder, minorder)
            morder = min(morder, maxorder)
            startup_cost = comparison_cost * tuples * LOG2(tuples)
            log_runs = 1

            if nruns > morder:
                log_runs = math.ceil(math.log(nruns)/math.log(morder))
            npage_access = 2*npages * log_runs

            startup_cost+= npage_access*(seq_page_cost*0.75 + random_page_cost*0.25)
        
        else:
            startup_cost = comparison_cost * tuples * LOG2(tuples)
        run_cost = cpu_operator_cost * tuples
        cost = run_cost+startup_cost

        qep_node.our_cost = cost
        return cost

    def _estimate_hashJoin_cost(self, qep_node: QEPNode) -> None:
        pass
        
        # sequential scan for the right child
        # for each record in right child, calculate the hash value and 
        # see if it matches with the hash value of the left child
        
        right_child = qep_node.right_child
        left_child = qep_node.left_child
        
        cost_right = right_child.our_cost
        
        # cost of hashing the right child
        right_size = right_child.our_size
        right_tuples = right_size / right_child.properties['Plan Width']
        right_cost = right_tuples * cpu_operator_cost
        
        # cost of comparing the hash values
        left_size = left_child.our_size
        left_tuples = left_size / left_child.properties['Plan Width']
        comparison_cost = 2 * cpu_operator_cost
        comparison_cost = comparison_cost * left_tuples
        
        # total cost
        cost = cost_right + right_cost + comparison_cost
        
        qep_node.our_cost = cost
        

    def _estimate_indexScan_cost(self, qep_node: QEPNode) -> None:
        index_corrs = self._get_index_corr(qep_node.properties['Relation Name'])
        print('index_corrs:', index_corrs)
        
        # find the column name that is being indexed from condition 
        condition = qep_node.properties['Index Cond'].replace('(', '').replace(')', '')
        attribute_name = condition.split(' ')[0].split('.')[1]
        print('attribute_name:', attribute_name)
        corr = index_corrs[attribute_name]
        
        index_selectivities = []
        conditions = qep_node.properties['Index Cond'].replace('(', '').replace(')', '').split(' AND ')
        table_name = qep_node.properties['Relation Name']
        for condition in conditions:
            attribute_name = condition.split(' ')[0].split('.')[1]
            if "=" in condition:
                selectivity = 1/self.num_distinct_dict[table_name][attribute_name]
            else:
                selectivity = 1/3
            index_selectivities.append(selectivity)
            
        print('index_selectivities:', index_selectivities)
        print('index_corrs:', index_corrs)
        num_tuples = self.tuple_count_dict[table_name]
        num_pages = num_tuples / (BLK_SIZE/self.row_width_dict[table_name])
        
        numIndexTuples = sum([i * num_tuples for i in index_selectivities])
        numIndexPages = sum([i * num_pages for i in index_selectivities])
        
        cost = numIndexPages * seq_page_cost + numIndexTuples * (cpu_index_tuple_cost + 0.001)
        DISCOUNT_FACTOR = 0.03 if qep_node.our_size < 1000 else 1

        qep_node.our_cost = cost * DISCOUNT_FACTOR * BLK_SIZE

    def _get_index_corr(self, table_name):
        query = f'''SELECT attname, correlation, n_distinct -- , null_frac
                    FROM pg_stats
                    WHERE tablename = '{table_name}'
                    AND schemaname = 'public';'''
        
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            index_corr = {}
            for row in rows:
                index_corr[row[0]] = abs(row[1])
            return index_corr
       
    def _estimate_bitmapScan_cost(self, qep_node: QEPNode):
        # Increase cost a little to represent the bitmap creation
        
        if 'Recheck Cond' in qep_node.properties:
            table_name = qep_node.properties['Recheck Cond'].replace(
                '(', '').replace(')', '').strip().split('.')[0]
            size = self.row_width_dict[table_name] * self.tuple_count_dict[table_name]
        elif 'Index Cond' in qep_node.properties:
            table_name = qep_node.properties['Index Cond'].replace(
                '(', '').replace(')', '').strip().split('.')[0]
            size = self.row_width_dict[table_name] * \
                self.tuple_count_dict[table_name]
        print("size ", size)
        startup_cost = 0.1*cpu_tuple_cost*qep_node.size
        print("startup_cost ", startup_cost)
        pages = size/ BLK_SIZE
        print("pages ", pages)
        loop_count = qep_node.properties['Actual Loops']
        print("loops ", loop_count)
        pages = pages/loop_count
        pages = math.ceil(pages)
        pages_cost = pages*random_page_cost
        print("pagescost ", pages_cost)
        qep_node.our_cost = pages_cost*0.01 + startup_cost * 12.5

    def _estimate_mergeJoin_cost(self, qep_node: QEPNode):
        cost = 0
        total_tuples = (qep_node.left_child.properties['Plan Rows'] + qep_node.right_child.properties['Plan Rows'])
        start_cost =  cpu_operator_cost * total_tuples
        run_cost = cpu_tuple_cost * total_tuples
        cost = start_cost + run_cost
        qep_node.our_cost = cost
        
    def _estimate_limit_cost(self, qep_node: QEPNode):
        # use regex to get limit value from query
        import re
        limit = int(re.findall(r'(?i)LIMIT\s+(\d+)', self.current_query)[0])
        print('Limit:', limit)

        prev_size = qep_node.left_child.our_size
        size = qep_node.size
        cost = size/prev_size * qep_node.left_child.our_cost
        print('Prev size:', prev_size)
        print('Size:', size)
        print('Prev cost:', qep_node.left_child.our_cost)
        print('Cost:', cost)
        qep_node.our_cost = round(cost, 3)

    def _estimate_nestedLoop_cost(self, qep_node: QEPNode):
        inner_rows = qep_node.left_child.properties['Actual Rows']
        outer_rows = qep_node.right_child.properties['Actual Rows']

        cost = inner_rows * outer_rows + min(inner_rows, outer_rows)
        cost *= cpu_tuple_cost
  
        qep_node.our_cost = cost
        
    def _get_node_cost(self, qep_node: QEPNode) -> float:
        """
        Compute the planner's estimate for the cost of each node
        """
        if qep_node == None:
            return 0

        cost_left_child = self._get_node_cost(qep_node.left_child)
        cost_right_child = self._get_node_cost(qep_node.right_child)

        if qep_node.node_type == 'Limit':
            current_node_cost = qep_node.properties['Total Cost']
        else:
            current_node_cost = qep_node.properties["Total Cost"] - \
                cost_left_child - cost_right_child

        qep_node.planner_cost = current_node_cost
        return qep_node.properties["Total Cost"]

    def _add_node_to_graph(self, node: QEPNode, parent: QEPNode = None) -> None:
        """
        Add a QEPNode to its graphical representation using iGraph
        """
        # Identifier
        node_id = str(node.node_id)
        # Attributes to display
        operation = node.node_type
        if 'Relation Name' in node.properties:
            if 'Alias' in node.properties and node.properties['Alias'] != node.properties['Relation Name']:
                relation = f'on {node.properties["Relation Name"]} as {node.properties["Alias"]}'
            else:
                relation = f'on {node.properties["Relation Name"]}'
        else:
            relation = ''
        condition = [
            f'{key}: {node.properties[key]}' for key in PG_CONDS if key in node.properties]
        exclude = '::' + '|::'.join(PG_TYPES)
        condition = [re.sub(exclude, "", cond)
                     for cond in condition]           # Type casting
        condition = [re.sub(r"[\[\]\'\"]", "", cond)
                     for cond in condition]       # Brackets, quotes
        condition = ',<br>'.join(condition) + \
            '<br>' if len(condition) > 0 else ''
        if self.current_qep.average_node_execution_time is not None:
            node_execution_time_multiplier = node.execution_time / self.current_qep.average_node_execution_time
            if node_execution_time_multiplier <= 0.75:
                node_color = 'seagreen'
            elif node_execution_time_multiplier <= 2:
                node_color = 'yellow'
            else:
                node_color = 'red'
        else:
            node_color = 'seagreen'

        self.qep_graph.add_vertex(
            node_id,
            label=operation,
            relation=relation,
            condition=condition,
            planner_cost=node.planner_cost,
            node_color=node_color,
            our_cost=node.our_cost,
            our_size=node.our_size,
            size=node.size
        )

        if parent:
            self.qep_graph.add_edges([(node_id, parent)])
        for child in [node.left_child, node.right_child]:
            if child:
                self._add_node_to_graph(child, node_id)

    def plot_qep_tree(self) -> go.Figure:
        """
        Organises and displays the QEP in a top-down tree structure based on the Reingold-Tilford algorithm.
        """
        self.qep_graph = Graph(directed=True)

        self._add_node_to_graph(self.current_qep.root)
        layout = self.qep_graph.layout_reingold_tilford(root=[0], mode='in')
        edges = self.qep_graph.get_edgelist()
        n_vertices = len(self.qep_graph.vs)

        position = {k: layout[k] for k in range(n_vertices)}
        Y = [layout[k][1] for k in range(n_vertices)]
        M = max(Y)
        L = len(position)
        X_nodes = [position[k][0] for k in range(L)]
        Y_nodes = [2 * M - position[k][1] for k in range(L)]
        X_edges, Y_edges = [], []
        for edge in edges:
            X_edges += [position[edge[0]][0], position[edge[1]][0], None]
            Y_edges += [2 * M - position[edge[0]][1],
                        2 * M - position[edge[1]][1], None]

        colors = {
            'seagreen': 'Fast',
            'yellow': 'Moderate',
            'red': 'Slow'
        }

        node_trace = go.Scatter(
            x=X_nodes,
            y=Y_nodes,
            mode='markers+text',
            marker=dict(
                symbol='circle',
                size=20,
                color=self.qep_graph.vs['node_color'],
                line=dict(width=2, color='darkslategrey')
            ),
            text=self.qep_graph.vs['label'],
            textposition='middle left',
            customdata=np.array(
                [self.qep_graph.vs['relation'],
                 self.qep_graph.vs['condition'],
                 self.qep_graph.vs['planner_cost'],
                 self.qep_graph.vs['our_cost'],
                 self.qep_graph.vs['our_size'],
                 self.qep_graph.vs['size']]
            ).T,
            hovertemplate='<b>%{text}</b> %{customdata[0]}<br>' +
            '%{customdata[1]}<br>' +
            'Node Cost (Planner): %{customdata[2]:.3f}<br>'
            'Node Cost (Our Estimate): %{customdata[3]:.3f}<br>'
            '<extra></extra>',
            hoverlabel=dict(
                bgcolor='white',
                bordercolor=self.qep_graph.vs['node_color'],
                font=dict(color='black', family='Open Sans, sans-serif')
            ),
            showlegend=False
        )

        dummy_traces = []
        for color in colors:
            dummy_trace = go.Scatter(
                x=[None],
                y=[None],
                mode='markers',
                marker=dict(
                    symbol='circle',
                    size=20,
                    color=color,
                    line=dict(width=2, color='darkslategrey')
                ),
                name=colors[color],
                showlegend=True
            )
            dummy_traces.append(dummy_trace)

        edge_trace = go.Scatter(
            x=X_edges,
            y=Y_edges,
            mode='markers+lines',
            line=dict(width=1, color='black'),
            hoverinfo='none',
            showlegend=False
        )

        fig = go.Figure(
            data=[edge_trace, node_trace, *dummy_traces],
            layout=dict(
                # showlegend=False,
                legend=dict(
                    xanchor='left',
                    x=0.01,
                    yanchor='top',
                    y=0.75,
                    bgcolor='white',
                    bordercolor='mediumseagreen',
                    borderwidth=1,
                    font_size=13,
                    title_font_size=13,
                    title_text='<b>Relative Execution Times</b>'
                ),
                xaxis=dict(showgrid=False, zeroline=False,
                           showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False,
                           showticklabels=False),
                autosize=True,
                height=600,
                hovermode='closest'
            )
        )

        annotation_text = f'<b>Query Execution Plan</b><br>' + \
                          f'Planning Time: {self.current_qep.planning_time:.3f} ms<br>Execution Time: {self.current_qep.execution_time:.3f} ms<br>' + \
                          f'Disk I/Os for Planning: {self.current_qep.shared_blocks_read}<br>Buffer Accesses for Planning: {self.current_qep.shared_blocks_hit}'
        
        
        fig.add_annotation(
            text=annotation_text,
            align='left',
            showarrow=False,
            xref='paper',
            yref='paper',
            x=0.01,
            y=0.94,
            borderpad=6,
            bgcolor='white',
            bordercolor='mediumseagreen',
            borderwidth=1,
            font_size=13
        )

        return fig

    def get_pages_accessed(self) -> dict[str, tuple[str, list[int]]]:
        """
        Parse the current QEP and return the page numbers of all pages (blocks) accessed by the QEP

        Returns {nodeID: (relationName, [pageNumbers])}
        """
        # Iterate through all plan nodes in qep and retrieve all scan nodes
        plans_stack = [self.current_qep.root]
        relation_aliases = {}
        scans = []
        while len(plans_stack) > 0:
            node = plans_stack.pop()
            nodeID = node.node_id
            nodeType = node.node_type
            relationName = node.properties.get("Relation Name", None)
            alias = node.properties.get("Alias", None)
            if relationName is not None:
                # If the relation has no alias, set its alias to the relation name
                relation_aliases.setdefault(
                    alias if alias is not None else relationName, relationName)
            if re.match(r'.*\sScan', nodeType):
                scan = {
                    "nodeID": nodeID,
                    "type": nodeType,
                    "relation": relationName,
                    "index": node.properties.get("Index Name", None),
                    "cond": node.properties.get("Index Cond", node.properties.get("Recheck Cond", None))
                }
                scans.append(scan)
            if node.left_child:
                plans_stack.append(node.left_child)
            if node.right_child:
                plans_stack.append(node.right_child)

        pagesAccessed = {}
        for scan in scans:
            relation = scan['relation']
            if relation is not None:
                # Either a Index Scan or Bitmap Heap Scan
                # Need to filter for the case of Index Only Scan since that doesn't access the relation pages
                if scan['cond'] is not None:
                    aliases = list(relation_aliases.keys())
                    aliases_involved = re.findall(
                        rf'({"|".join(aliases)})\.', scan['cond'])
                    relations_involved_aliased = [
                        f"{relation_aliases[alias]} {alias}" for alias in aliases_involved]
                    from_clause = ",".join(relations_involved_aliased)
                    relation_aliases_inversed = {
                        v: k for k, v in relation_aliases.items()}
                    query = \
                        f"""
                    SELECT DISTINCT ({relation_aliases_inversed[relation]}.ctid::text::point)[0]::int AS ctid
                    FROM {from_clause}
                    WHERE {scan['cond']}
                    ORDER BY ctid
                    """
                else:
                    # Sequential Scan
                    query = \
                        f"""
                    SELECT DISTINCT (ctid::text::point)[0]::int AS ctid
                    FROM {relation}
                    ORDER BY ctid
                    """
                with self.conn.cursor() as cur:
                    result = cur.execute(query)
                    pagesAccessed[scan['nodeID']] = (
                        relation, [tuple[0] for tuple in result.fetchall()])
        return pagesAccessed

    def get_page_contents(self, relationName: str, pageNumber: int) -> tuple[list[str], list[Any]]:
        """
        Retrieve the records contained in a page specified by the pageNumber of relationName
        """
        query = \
            f"""
        SELECT *
        FROM {relationName}
        WHERE (ctid::text::point)[0]::int = {pageNumber}
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            data = cur.fetchall()
            headers = [desc[0] for desc in cur.description]
        return headers, data


    def get_num_distinct_values(self):
        num_distinct_dict = {}

        # Cursor to execute queries
        cur = self.conn.cursor()

        # Query to get all table names in the current database (excluding system tables)
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cur.fetchall()

        # For each table, get the distinct count of each column
        for (table_name,) in tables:
            cur.execute(f"SELECT * FROM {table_name} LIMIT 0")
            column_names = [desc[0] for desc in cur.description]

            num_distinct_dict[table_name] = {}

            for column in column_names:
                cur.execute(f"SELECT COUNT(DISTINCT {column}) FROM {table_name}")
                distinct_count = cur.fetchone()[0]
                num_distinct_dict[table_name][column] = distinct_count

        return num_distinct_dict


    def get_tuple_counts(self):
        tuple_count_dict = {}

        # Cursor to execute queries
        cur = self.conn.cursor()

        # Query to get all table names in the current database (excluding system tables)
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cur.fetchall()

        # For each table, get the count of tuples
        for (table_name,) in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cur.fetchone()[0]
            tuple_count_dict[table_name] = row_count

        return tuple_count_dict


    def get_average_row_widths(self):
        row_width_dict = {}

        # Cursor to execute queries
        cur = self.conn.cursor()

        # Query to get all table names in the current database (excluding system tables)
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cur.fetchall()

        # For each table, calculate the average row width
        for (table_name,) in tables:
            # Calculate the average row width using the pg_column_size function
            cur.execute(f"""
                SELECT AVG(pg_column_size(t.*)) 
                FROM {table_name} AS t
            """)
            avg_row_width = cur.fetchone()[0]
            row_width_dict[table_name] = int(
                avg_row_width) if avg_row_width is not None else None

        return row_width_dict



if __name__ == '__main__':

    pgserver = PGServer()

    pgserver.connect(host='localhost', port='5432', database='TCP-H', user='postgres', password='RVPSRTscm20!?')

    query = "SELECT * FROM partsupp JOIN supplier ON partsupp.ps_suppkey = supplier.s_suppkey ORDER BY partsupp.ps_suppkey;"
    # query = 'SELECT * FROM nation n INNER JOIN customer c ON n.n_nationkey = c.c_nationkey WHERE n.n_regionkey = 2 LIMIT 10;'

    #query = query_list[1]

    options = ['ANALYZE']


    qep = pgserver.get_qep(query=query, options=options)
    qep.root.update_sizes()

    print(qep.show())