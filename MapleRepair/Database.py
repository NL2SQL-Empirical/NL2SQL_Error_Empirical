import re
import csv
import sqlite3
from typing import Dict, Any, Tuple
from qdrant_client import QdrantClient
from typing import List, Optional, Union, Tuple
from copy import deepcopy
import pickle
from MapleRepair.utils.format import read_json
import time
from MapleRepair.utils.persistence import make_log
import json

from MapleRepair.config import dataset
from MapleRepair.dataset_utils.bird import Bird_Initializer
from MapleRepair.dataset_utils.spider import Spider_Initalizer

from MapleRepair.config import *
from MapleRepair.utils.nlp import is_date, is_time, is_number
from MapleRepair.utils.ds import TableColumnPair, DSU
from MapleRepair.Customized_Exception import NoSuchTableError, NoSuchColumnError
from MapleRepair.order_check import _is_orderable
from func_timeout import func_set_timeout, FunctionTimedOut

from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures import as_completed

import base64

def encode_string(input_string):
    byte_string = input_string.encode('utf-8')
    encoded_string = base64.urlsafe_b64encode(byte_string).decode('utf-8')
    return encoded_string

def decode_string(encoded_string):
    byte_string = base64.urlsafe_b64decode(encoded_string.encode('utf-8'))
    return byte_string.decode('utf-8')

def load_data(path:Path) -> Tuple[Any, bool]:
    if not path.exists() or not persistence:
        return None, False
    
    fp = path.open('rb')
    loaded_data = pickle.load(fp)
    fp.close()
    return loaded_data, True

def save_data(path:Path, data) -> None:
    if not persistence:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
        
    fp = path.open('wb')
    pickle.dump(data, fp)
    fp.close()

def is_email(string):
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    match = re.match(pattern, string)
    if match:
        return True
    else:
        return False
    
def is_valid_date(date_str):
    if (not isinstance(date_str, str)):
        return False
    date_str = date_str.split()[0]
    if len(date_str) != 10:
        return False
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    if re.match(pattern, date_str):
        year, month, day = map(int, date_str.split('-'))
        if year < 1 or month < 1 or month > 12 or day < 1 or day > 31:
            return False
        else:
            return True
    else:
        return False

def is_valid_date_column(col_value_lst):
    for col_value in col_value_lst:
        if not is_valid_date(col_value):
            return False
    return True

def _eq(s1:str, s2:str) -> bool:
    """
    case insensitive string eq.
    """
    if s1.lower() == s2.lower():
        return True
    return False

class Schema_Prompt_Builder():
    """
    Get database description and if need, extract relative tables & columns
    """

    def __init__(self, _dataset:str, _data_split:str, db_path: str, db_id:str):
        self.db_path = db_path
        self.dataset_name = _dataset
        self.data_split = _data_split
        self.db_id = db_id
        self.db2info = self._load_single_db_info(db_id)  # summary of db (stay in the memory during generating prompt)
        # this class is thread safe, since all vars here are read-only after init.
        assert self.db2info is not None, f"db2info is None, db_id: {db_id}"
    
    def _get_column_attributes(self, cursor, table):
        # # 查询表格的列属性信息
        cursor.execute(f"PRAGMA table_info(`{table}`)")
        columns = cursor.fetchall()

        # 构建列属性信息的字典列表
        columns_info = []
        primary_keys = []
        column_names = []
        column_types = []
        for column in columns:
            column_names.append(column[1])
            column_types.append(column[2].split('(')[0].upper())
            is_pk = bool(column[5])
            if is_pk:
                primary_keys.append(column[1])
            column_info = {
                'name': column[1],  # 列名
                'type': column[2].split('(')[0].upper(),  # 数据类型
                'not_null': bool(column[3]),  # 是否允许为空
                'primary_key': bool(column[5])  # 是否为主键
            }
            columns_info.append(column_info)
        """
        table: satscores
        [{'name': 'cds', 'not_null': True, 'primary_key': True, 'type': 'TEXT'},
        {'name': 'rtype', 'not_null': True, 'primary_key': False, 'type': 'TEXT'},
        {'name': 'sname', 'not_null': False, 'primary_key': False, 'type': 'TEXT'},
        {'name': 'dname', 'not_null': False, 'primary_key': False, 'type': 'TEXT'},
        {'name': 'cname', 'not_null': False, 'primary_key': False, 'type': 'TEXT'},
        {'name': 'enroll12','not_null': True, 'primary_key': False, 'type': 'INTEGER'},
        ...
        """
        return column_names, column_types
    
    def _save_unique_column_values_str(self, table, column_names, values):
        table = encode_string(table)
        column_names = encode_string(column_names)
        dataset_name = encode_string(self.dataset_name)
        _data_split = encode_string(self.data_split)
        db_id = encode_string(self.db_id)
        path = Path(db_cache_dir)/f"{dataset_name}"/f"{_data_split}"/f"{db_id}"/f"{table}"/f"{column_names}/unique_values_str.pickle"
        save_data(path, values)
    
    def _load_unique_column_values_str(self, table, column_names) -> Optional[List]:
        table = encode_string(table)
        column_names = encode_string(column_names)
        dataset_name = encode_string(self.dataset_name)
        _data_split = encode_string(self.data_split)
        db_id = encode_string(self.db_id)
        path = Path(db_cache_dir)/f"{dataset_name}"/f"{_data_split}"/f"{db_id}"/f"{table}"/f"{column_names}/unique_values_str.pickle"
        values, res = load_data(path)
        return values
    
    def _get_unique_column_values_str(self, cursor, table, column_names, column_types, is_key_column_lst):

        col_to_values_str_lst = []
        col_to_values_str_dict = {}

        key_col_list = [column_names[i] for i, flag in enumerate(is_key_column_lst) if flag]

        len_column_names = len(column_names)

        for idx, column_name in enumerate(column_names):
            # 查询每列的 distinct value, 从指定的表中选择指定列的值，并按照该列的值进行分组。然后按照每个分组中的记录数量进行降序排序。
            # print(f"In _get_unique_column_values_str, processing column: {idx}/{len_column_names} col_name: {column_name} of table: {table}", flush=True)

            # skip pk and fk
            if column_name in key_col_list:
                continue
            
            lower_column_name: str = column_name.lower()
            # if lower_column_name ends with [id, email, url], just use empty str
            if lower_column_name.endswith('id') or \
                lower_column_name.endswith('email') or \
                lower_column_name.endswith('url'):
                values_str = ''
                col_to_values_str_dict[column_name] = values_str
                continue

            # FIXME: SPEED UP!!!
            values = self._load_unique_column_values_str(table, column_name)
            if not values:
                sql = f"SELECT `{column_name}` FROM `{table}` GROUP BY `{column_name}` ORDER BY COUNT(*) DESC LIMIT 100"
                cursor.execute(sql)
                values = cursor.fetchall()
                values = [value[0] for value in values]
                self._save_unique_column_values_str(table, column_name, values)

            values_str = ''
            # try to get value examples str, if exception, just use empty str
            try:
                values_str = self._get_value_examples_str(values, column_types[idx])
            except Exception as e:
                print(f"\nerror: get_value_examples_str failed, Exception:\n{e}\n")

            col_to_values_str_dict[column_name] = values_str


        for k, column_name in enumerate(column_names):
            values_str = ''
            # print(f"column_name: {column_name}")
            # print(f"col_to_values_str_dict: {col_to_values_str_dict}")

            is_key = is_key_column_lst[k]

            # pk or fk do not need value str
            if is_key:
                values_str = ''
            elif column_name in col_to_values_str_dict:
                values_str = col_to_values_str_dict[column_name]
            else:
                print(col_to_values_str_dict)
                # sleep(3)
                print(f"error: column_name: {column_name} not found in col_to_values_str_dict")
            
            col_to_values_str_lst.append([column_name, values_str])
        
        return col_to_values_str_lst
    
    # 这个地方需要精细化处理
    def _get_value_examples_str(self, values: List[object], col_type: str):
        if not values:
            return ''
        col_type = col_type.split('(')[0].upper()
        # print(col_type)
        if len(values) > 10 and col_type in ['INTEGER', 'REAL', 'NUMERIC', 'FLOAT', 'INT']:
            return ''
        
        vals = []
        has_null = False
        for v in values:
            if v is None:
                has_null = True
            else:
                tmp_v = str(v).strip()
                if tmp_v == '':
                    continue
                else:
                    vals.append(v)
        if not vals:
            return ''
        
        # drop meaningless values
        if col_type in ['TEXT', 'VARCHAR']:
            new_values = []
            
            for v in vals:
                if not isinstance(v, str):
                    new_values.append(v)
                else:
                    if self.dataset_name.lower() == 'spider':
                        v = v.strip()
                    if v == '': # exclude empty string
                        continue
                    elif ('https://' in v) or ('http://' in v): # exclude url
                        return ''
                    elif is_email(v): # exclude email
                        return ''
                    else:
                        new_values.append(v)
            vals = new_values
            tmp_vals = [len(str(a)) for a in vals]
            if not tmp_vals:
                return ''
            max_len = max(tmp_vals)
            if max_len > 50:
                return ''
        
        if not vals:
            return ''
        
        vals = vals[:6]

        is_date_column = is_valid_date_column(vals)
        if is_date_column:
            vals = vals[:1]

        if has_null:
            vals.insert(0, None)
        
        val_str = str(vals)
        return val_str
    
    def _load_single_db_info(self, db_id: str) -> dict:
        """
        table2col_description: Dict {
            table_name: [(column_name, column_description), ...]
        }
        table2primary_keys: Dict {
            table_name: [primary_key_column_name,...]
        }
        table_foreign_keys: Dict {
            table_name: [(from_col, to_table, to_col), ...]
        }
        table_unique_column_values: Dict {
            table_name: [(column_name, column_values), ...]
        }
        """
        assert DBs.get(db_id) is not None
        dbs = DBs[db_id]
        
        # table2coldescription = {} # Dict {table_name: [(column_name, full_column_name, column_description), ...]}
        table2coldescription = {} # Dict {table_name: [(column_name, type, column_description, value_description), ...]}
        # table2primary_keys = {} # DIct {table_name: [primary_key_column_name,...]}
        table2primary_keys = dbs.pkfk['pk_dict']    # pass diff test (with MAC-SQL Impl.)
        
        # table_foreign_keys = {} # Dict {table_name: [(from_col, to_table, to_col), ...]}
        
        # pass diff test (with MAC-SQL Impl.).
        # Inconsistenct with 'european_football_2' will cause problem.
        table_foreign_keys = {}
        for from_table, fks in dbs.pkfk['fk_dict'].items():
            if from_table not in table_foreign_keys:
                table_foreign_keys[from_table] = []
            for fk in fks:
                table_foreign_keys[from_table].append((fk['from_col'], fk['to_table'], fk['to_col']))
        
        table_unique_column_values = {} # Dict {table_name: [(column_name, examples_values_str)]}

        # db_dict = self.db2dbjsons[db_id]
        
        def is_pk(col:str) -> bool:
            for table, pks in table2primary_keys.items():
                for pk in pks:
                    if col.lower() == pk.lower():
                        return True
            return False
        
        def is_fk(col:str) -> bool:
            for table, fks in table_foreign_keys.items():
                for fk in fks:
                    if col.lower() in (fk[0].lower(), fk[2].lower()):
                        return True
            return False
        
        important_key_name_set = set()
        for table in dbs.schema.keys():
            for col in dbs.schema[table].keys():
                if is_pk(col) or is_fk(col):
                    important_key_name_set.add(col)

        conn = sqlite3.connect(self.db_path, isolation_level=None)
        conn.text_factory = lambda b: b.decode(errors="ignore")  # avoid gbk/utf8 error, copied from sql-eval.exec_eval
        cursor = conn.cursor()
        
        for tb_name in dbs.schema.keys():
            ### get desc
            ...
            if tb_name not in table2coldescription:
                table2coldescription[tb_name] = []
            for col_name in dbs.schema[tb_name].keys():
                desc = dbs.schema[tb_name][col_name]['description']
                _type = dbs.schema[tb_name][col_name]['type']
                table2coldescription[tb_name].append(
                    (col_name, _type, desc)
                )
            
            ### get value
            all_sqlite_column_names_lst = []
            is_key_column_lst = []
            for col_name in dbs.schema[tb_name].keys():
                all_sqlite_column_names_lst.append(col_name)
                is_key_column_lst.append(True if col_name in important_key_name_set else False)
                
            table_unique_column_values[tb_name] = []

            # column_names, column_types
            all_sqlite_column_names_lst, all_sqlite_column_types_lst = self._get_column_attributes(cursor, tb_name)
            col_to_values_str_lst = self._get_unique_column_values_str(cursor, tb_name, all_sqlite_column_names_lst, all_sqlite_column_types_lst, is_key_column_lst)
            table_unique_column_values[tb_name] = col_to_values_str_lst
        
        cursor.close()
        # print table_name and primary keys
        # for tb_name, pk_keys in table2primary_keys.items():
        #     print(f"table_name: {tb_name}; primary key: {pk_keys}")
        # time.sleep(3)

        # wrap result and return
        result = {
            "desc_dict": table2coldescription,
            "value_dict": table_unique_column_values,
            "pk_dict": table2primary_keys,  # { "table": [pk1, pk2, ...], ... }
            "fk_dict": table_foreign_keys   # { "from table": [ (from col, to table, to col), ... ] }
        }
            
        return result
    
    
    def _build_bird_table_schema_sqlite_str(self, table_name, new_columns_desc, new_columns_val):
        schema_desc_str = ''
        schema_desc_str += f"CREATE TABLE {table_name}\n"
        extracted_column_infos = []
        for (col_name, full_col_name, col_extra_desc), (_, col_values_str) in zip(new_columns_desc, new_columns_val):
            # district_id INTEGER PRIMARY KEY, -- location of branch
            col_line_text = ''
            col_extra_desc = 'And ' + str(col_extra_desc) if col_extra_desc != '' and str(col_extra_desc) != 'nan' else ''
            col_extra_desc = col_extra_desc[:100]
            col_line_text = ''
            col_line_text += f"  {col_name},  --"
            if full_col_name != '':
                full_col_name = full_col_name.strip()
                col_line_text += f" {full_col_name},"
            if col_values_str != '':
                col_line_text += f" Value examples: {col_values_str}."
            if col_extra_desc != '':
                col_line_text += f" {col_extra_desc}"
            extracted_column_infos.append(col_line_text)
        schema_desc_str += '{\n' + '\n'.join(extracted_column_infos) + '\n}' + '\n'
        return schema_desc_str
    
    def _build_bird_table_schema_list_str(self, table_name, new_columns_desc, new_columns_val):
        schema_desc_str = ''
        schema_desc_str += f"# Table: {table_name}\n"
        extracted_column_infos = []
        for (col_name, _type, col_desc), (_, col_values_str) in zip(new_columns_desc, new_columns_val):
            if col_desc.lower() != col_name.lower():
                col_desc = 'which means ' + str(col_desc) if col_desc != '' and str(col_desc) != 'nan' else ''
            else:
                col_desc = "there's no description of it"
            col_desc = col_desc[:100]

            col_line_text = ''
            col_line_text += f'  ('
            col_line_text += f"{col_name},"

            if col_desc != '':
                col_line_text += f" {col_desc}."
            if _type != '':
                _type = _type.strip()
                col_line_text += f" {_type}."
            if col_values_str != '':
                col_line_text += f" Value examples: {col_values_str}."
            col_line_text += '),'
            extracted_column_infos.append(col_line_text)
        schema_desc_str += '[\n' + '\n'.join(extracted_column_infos).strip(',') + '\n]' + '\n'
        return schema_desc_str
    
    def _get_db_desc_str(self,
                         extracted_schema: dict,
                         use_gold_schema: bool = False) -> List[str]:
        """
        Add foreign keys, and value descriptions of focused columns.
        :param db_id: name of sqlite database
        :param extracted_schema: {table_name: "keep_all" or "drop_all" or ['col_a', 'col_b']}
        :return: Detailed columns info of db; foreign keys info of db
        """
        db_info = self.db2info
        desc_info = db_info['desc_dict']  # table:str -> columns[(column_name, full_column_name, extra_column_desc): str]
        value_info = db_info['value_dict']  # table:str -> columns[(column_name, value_examples_str): str]
        pk_info = db_info['pk_dict']  # table:str -> primary keys[column_name: str]
        fk_info = db_info['fk_dict']  # table:str -> foreign keys[(column_name, to_table, to_column): str]
        tables_1, tables_2, tables_3 = desc_info.keys(), value_info.keys(), fk_info.keys()
        assert set(tables_1) == set(tables_2)
        assert set(tables_2) == set(tables_3)

        # print(f"desc_info: {desc_info}\n\n")

        # schema_desc_str = f"[db_id]: {db_id}\n"
        schema_desc_str = ''  # for concat
        db_fk_infos = []  # use list type for unique check in db

        # print(f"extracted_schema:\n")
        # pprint(extracted_schema)
        # print()

        # print(f"db_id: {db_id}")
        # For selector recall and compression rate calculation
        chosen_db_schem_dict = {} # {table_name: ['col_a', 'col_b'], ..}
        for (table_name, columns_desc), (_, columns_val), (_, fk_info), (_, pk_info) in \
                zip(desc_info.items(), value_info.items(), fk_info.items(), pk_info.items()):
            
            table_decision = extracted_schema.get(table_name, '')
            if table_decision == '' and use_gold_schema:
                continue

            # columns_desc = [(column_name, full_column_name, extra_column_desc): str]
            # columns_val = [(column_name, value_examples_str): str]
            # fk_info = [(column_name, to_table, to_column): str]
            # pk_info = [column_name: str]

            all_columns = [name for name, _, _ in columns_desc]
            primary_key_columns = [name for name in pk_info]
            foreign_key_columns = [name for name, _, _ in fk_info]

            important_keys = primary_key_columns + foreign_key_columns

            new_columns_desc = []
            new_columns_val = []

            # print(f"table_name: {table_name}")
            if table_decision == "drop_all":
                new_columns_desc = deepcopy(columns_desc[:6])
                new_columns_val = deepcopy(columns_val[:6])
            elif table_decision == "keep_all" or table_decision == '':
                new_columns_desc = deepcopy(columns_desc)
                new_columns_val = deepcopy(columns_val)
            else:
                llm_chosen_columns = table_decision
                # print(f"llm_chosen_columns: {llm_chosen_columns}")
                append_col_names = []
                for idx, col in enumerate(all_columns):
                    # if col in important_keys:
                    for key in important_keys:
                        if _eq(col, key):
                            new_columns_desc.append(columns_desc[idx])
                            new_columns_val.append(columns_val[idx])
                            append_col_names.append(col)
                            break
                    # elif col in llm_chosen_columns:
                    for llm_col in llm_chosen_columns:
                        if _eq(col, llm_col):
                            new_columns_desc.append(columns_desc[idx])
                            new_columns_val.append(columns_val[idx])
                            append_col_names.append(col)
                            break
                    else:
                        pass
                
                # todo: check if len(new_columns_val) ≈ 6
                if len(all_columns) > 6 and len(new_columns_val) < 6:
                    for idx, col in enumerate(all_columns):
                        if len(append_col_names) >= 6:
                            break
                        if col not in append_col_names:
                            new_columns_desc.append(columns_desc[idx])
                            new_columns_val.append(columns_val[idx])
                            append_col_names.append(col)

            # 统计经过 Selector 筛选后的表格信息
            chosen_db_schem_dict[table_name] = [col_name for col_name, _, _ in new_columns_desc]
            
            # 1. Build schema part of prompt
            # schema_desc_str += self._build_bird_table_schema_sqlite_str(table_name, new_columns_desc, new_columns_val)
            schema_desc_str += self._build_bird_table_schema_list_str(table_name, new_columns_desc, new_columns_val)

            # 2. Build foreign key part of prompt
            for col_name, to_table, to_col in fk_info:
                from_table = table_name
                if '`' not in str(col_name):
                    col_name = f"`{col_name}`"
                if '`' not in str(to_col):
                    to_col = f"`{to_col}`"
                fk_link_str = f"{from_table}.{col_name} references {to_table}.{to_col}, {from_table}.{col_name} = {to_table}.{to_col}"
                if fk_link_str not in db_fk_infos:
                    db_fk_infos.append(fk_link_str)
        fk_desc_str = '\n'.join(db_fk_infos)
        schema_desc_str = schema_desc_str.strip()
        fk_desc_str = fk_desc_str.strip()
        
        return schema_desc_str, fk_desc_str, chosen_db_schem_dict
    
    def get_schema_str(self):
        """
            thread safe method. (see __init__)
        """
        db_schema, db_fk, chosen_db_schem_dict = self._get_db_desc_str(extracted_schema={}, use_gold_schema=False)
        # print(db_schema)
        # print(db_fk)
        return db_schema, db_fk
class Database():    
    def __init__(self, db_id:str, _dataset:str, _data_split:str):
        print(f'Initializing database {db_id}...')
        self.db_id = db_id
        self.db_path = Path(db_root_path) / f'{db_id}/{db_id}.sqlite'
        
        # This should be lowercase since it will be used.
        self.dataset = _dataset
        self.data_split = _data_split
        
        if self.dataset == 'BIRD':
            self.schema, self.pkfk = Bird_Initializer(db_id).do_init()
        elif self.dataset == 'SPIDER':
            self.schema, self.pkfk = Spider_Initalizer(db_id).do_init()
        else:
            raise Exception
        
        # self.conn:sqlite3.Connection = None

        if not self.load_database():
            self.init_orderable()
            self.add_date_time_format()
            self.add_distinct_value()
            self.save_database()
            
        self._schema4sqlglot = self.init_schema4sqlglot()
        
        #HACK: better implementation
        self.schema_prompt = None
        self.fk_prompt = None
           
        self.init_disjoint_set()
        assert self.disjoint_set is not None
        self.init_fk_relationship() #NOTE for testing purpose
        assert self.fk_relationship is not None
        
        # after init, close conn -> Resources like Connection can not be transfer between process
        # self.conn.close()
        # self.conn:sqlite3.Connection = None
        
    def init_vecDB(self) -> None:
        # For parallel, sources (vector database) can not be transfer between process!
        # must be call after __init__
        self.vecDB_client = QdrantClient("localhost", port=6333)
        self.column_desc_vectorize()
        self.distinct_val_vectorize()
        
    def init_schema4sqlglot(self) -> dict:
        schema = {}
        tables = self.execute_query("SELECT name FROM sqlite_master WHERE type='table';")
        for table in tables:
            table_name = table[0]
            columns = self.execute_query(f"PRAGMA table_info(`{table_name}`);")
            schema[table_name] = {column[1]:column[2] for column in columns}
        return schema
    
    def add_date_time_format(self) -> None:
        for table in self.schema.keys():
            for col in self.schema[table].keys():
                if self.schema[table][col]['type'] not in ('TEXT', 'DATE'):
                    continue
                sample_data = self.execute_query(f"SELECT `{col}` FROM `{table}` \
                                                   WHERE `{col}` IS NOT NULL AND `{col}` != '' \
                                                   LIMIT 1")
                if not sample_data:
                    continue
                #NOTE: 日期格式的识别
                sample_data = sample_data[0][0]
                if not isinstance(sample_data, str):
                    sample_data = str(sample_data)
                
                if is_date(sample_data) or self.schema[table][col]['type'] == 'DATE':
                    for date_pattern,  strftime_pattern in DATE_FORMATS:
                        try:
                            if re.match(date_pattern, sample_data):
                                self.schema[table][col]['date_format'] = (date_pattern, strftime_pattern)
                                print(f'{table}.{col} date format: {date_pattern}')
                                break
                        except Exception as e:
                            print(f'Error when matching date pattern {date_pattern} on {sample_data}')
                            print(f'db_id: {self.db_id}, table: {table}, col: {col}')
                            raise e
                    
                #NOTE: 时间格式的识别 
                #HACK: M:SS.SSS is not recognized as time
                if is_time(sample_data) or self.schema[table][col]['type'] == 'TEXT':
                    for time_pattern, strftime_pattern in TIME_FORMATS:
                        try:
                            if re.match(time_pattern, sample_data):
                                self.schema[table][col]['time_format'] = (time_pattern, strftime_pattern)
                                print(f'{table}.{col} time format: {time_pattern}')
                                break
                        except Exception as e:
                            print(f'Error when matching time pattern {time_pattern} on {sample_data}')
                            print(f'db_id: {self.db_id}, table: {table}, col: {col}')
                            raise e
                    
    def distinct_val_vectorize(self) -> None:
        for table in self.schema.keys():
            for col in self.schema[table].keys():
                if self.schema[table][col]['distinct_val'] is not None:
                    collection_name = f"{self.dataset}.{self.data_split}.{self.db_id}.{table}.{col}.distinct_val"
                    if self.vecDB_client.collection_exists(collection_name):
                        dv_info = self.vecDB_client.get_collection(collection_name)
                        if dv_info.points_count != len(self.schema[table][col]['distinct_val']):
                            self.vecDB_client.delete_collection(collection_name)
                    if self.vecDB_client.collection_exists(collection_name) == False:
                        print(f"Vectorizing {collection_name}, sample: {list(self.schema[table][col]['distinct_val'])[0][:50]}")
                        self.vecDB_client.add(
                            collection_name=collection_name,
                            documents=list(self.schema[table][col]['distinct_val']),
                            metadata=[{"dataset":self.dataset, "data_split":self.data_split, "db_id":self.db_id, "table_name":table, "column_name":col}]*len(self.schema[table][col]['distinct_val'])
                        )
    
    def delete_distinct_val_collection(self):
        for table in self.schema.keys():
            for col in self.schema[table].keys():
                if self.schema[table][col]['distinct_val'] is not None:
                    collection_name = f"{self.dataset}.{self.data_split}.{self.db_id}.{table}.{col}.distinct_val"
                    if self.vecDB_client.collection_exists(collection_name):
                        self.vecDB_client.delete_collection(collection_name)
    
    def val_vec_query(self, table:str, column:str, text, top_k:int=3):
        assert self.get_column_info(table, column)
        for table_name in self.schema.keys():
            if table_name.lower() == table.lower():
                table = table_name
        for column_name in self.schema[table].keys():
            if column_name.lower() == column.lower():
                column = column_name
        query_results = self.vecDB_client.query(collection_name=f"{self.dataset}.{self.data_split}.{self.db_id}.{table}.{column}.distinct_val", query_text=text, limit=top_k)
        results = []
        for query_result in query_results:
            results.append(
                (
                    query_result.metadata['document'],
                    query_result.score
                )
            )
        return results
                
    def column_desc_vectorize(self) -> None:
        col_count = 0
        for table in self.schema.keys():
            for col in self.schema[table].keys():
                col_count += 1
                
        collection_name = f"{self.dataset}.{self.data_split}.{self.db_id}.column_description"
                
        if self.vecDB_client.collection_exists(collection_name):
            col_desc_collection_info = self.vecDB_client.get_collection(collection_name)
            if col_desc_collection_info.points_count != col_count:
                self.vecDB_client.delete_collection(collection_name=collection_name)
            
        if self.vecDB_client.collection_exists(collection_name) == False:
            for table in self.schema.keys():
                for col in self.schema[table].keys():
                    description = self.schema[table][col]['description']
                    self.vecDB_client.add(
                        collection_name=collection_name,
                        documents=[description],
                        metadata=[{"dataset":self.dataset, "data_split":self.data_split, "db_id":self.db_id, "table_name":table, "column_name":col}]
                    )
                    
        col_desc_collection_info = self.vecDB_client.get_collection(f"{self.dataset}.{self.data_split}.{self.db_id}.column_description")
        assert col_desc_collection_info.points_count == col_count
    
    def delete_column_desc_collection(self) -> None:
        collection_name = f"{self.dataset}.{self.data_split}.{self.db_id}.column_description"
        if self.vecDB_client.collection_exists(collection_name):
            self.vecDB_client.delete_collection(collection_name)

    def col_vec_query(self, text, top_k):
        """
            [
                {table_name:str, col_name:str, description:str, distance:float},
            ]
            result is in ascending order of distance
        """
        collection_name = f"{self.dataset}.{self.data_split}.{self.db_id}.column_description"
        result = []
        query_results = self.vecDB_client.query(collection_name=collection_name,query_text=text, limit=top_k)
        for query_result in query_results:
            result.append({
                'dataset': query_result.metadata['dataset'],
                'data_spilit': query_result.metadata['data_split'],
                'db_id': query_result.metadata['db_id'],
                'table_name': query_result.metadata['table_name'],
                'col_name': query_result.metadata['column_name'],
                'description': query_result.metadata['document'],
                'score': query_result.score
            })
        return result
    
    def execute_query(self, query:str, fetch: Optional[Union[str, int]] = "all", idx:Optional[int]=None) -> Optional[List]:   
        """
        Args:
            query (str):
            fetch (str):
                "all" (default): fetch all rows  
                "one": fetch 1 row  
                n (int): fetch n row(s)  
                None: no fetch  
            idx (Optional[int]):  
                for log purpose.  
        Returns:
            `List` if fetch else `None`
        """
        if idx is None:
            return self._execute_query(query, fetch)
        
        path = result_root_dir / "db_overhead" / f"{idx}.json"
        
        overhead = []
        if path.exists():
            overhead = read_json(path)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('[]')
        
        start = time.perf_counter()
        try:
            res = self._execute_query(query, fetch)
        finally:
            end = time.perf_counter()
            used_time = end - start
            overhead.append({"query": query, "time": used_time})
            content = json.dumps(overhead)
            make_log(path, content)
        
        return res
    
    @func_set_timeout(60)
    def _execute_query(self, query:str, fetch: Optional[Union[str, int]] = "all") -> Optional[List]:
        with sqlite3.connect(self.db_path, isolation_level=None) as conn:
            conn.text_factory = lambda b: b.decode(errors="ignore")  # avoid gbk/utf8 error, copied from sql-eval.exec_eval
            cursor = conn.cursor()
            cursor.execute(query)
            if not fetch:
                return None
            if fetch == "all":
                rows = cursor.fetchall()
            elif fetch == "one":
                rows = cursor.fetchone()
            elif isinstance(fetch, int):
                rows = cursor.fetchmany(fetch)
            return rows
        
    def is_executable(self, query:str, idx:Optional[int]=None) -> None:
        """
        Whether a SQL query is executable.
        
        Args:
            idx (Optional[int]):  
                for log purpose. 
            
        Exceptions:
            if SQL query is not executable, raise Exception.
        """
        query = f"EXPLAIN {query}"
        self.execute_query(query, fetch=None, idx=idx)
        # with sqlite3.connect(self.db_path, isolation_level=None) as conn:
        #     cursor = conn.cursor()
        #     query = f"EXPLAIN {query}"
        #     cursor.execute(query)
        
    def get_distinct_value_chess(self, table_name:str, column:str) -> Optional[set]:
        if any(keyword in column.lower() for keyword in ["_id", " id", "url", "email", "web", "time", "phone", "date", "address"]) or column.endswith("Id"):
            return None

        # FIXME: wired calculation?
        result = self.execute_query(f"""
            SELECT SUM(LENGTH(unique_values)), COUNT(unique_values)
            FROM (
                SELECT DISTINCT `{column}` AS unique_values
                FROM `{table_name}`
                WHERE `{column}` IS NOT NULL
            ) AS subquery
        """)

        sum_of_lengths, count_distinct = result[0]
        if sum_of_lengths is None or count_distinct == 0:
            return None

        # FIXME: what average length mean?
        average_length = sum_of_lengths / count_distinct
        # logging.info(f"Column: {column}, sum_of_lengths: {sum_of_lengths}, count_distinct: {count_distinct}, average_length: {average_length}")
        
        if ("name" in column.lower() and sum_of_lengths < 5000000) or (sum_of_lengths < 2000000 and average_length < 25):
            # logging.info(f"Fetching distinct values for {column}")
            values = [str(value[0]) for value in self.execute_query(f"SELECT DISTINCT `{column}` FROM `{table_name}` WHERE `{column}` IS NOT NULL")]
            # logging.info(f"Number of different values: {len(values)}")
            # print(f"{column}[{len(values)}]: {values[:10]}")
            return set(values)
        
        return None
    
    def get_distinct_value(self, table:str, col:str) -> Optional[set]:
        total_count = f"SELECT COUNT(`{col}`) FROM `{table}` WHERE `{col}` IS NOT NULL AND `{col}` != ''"
        total_count = self.execute_query(total_count)[0][0]
        
        if total_count == 0:
            return None
        
        distinct_count = f"SELECT COUNT(DISTINCT `{col}`) FROM `{table}` WHERE `{col}` IS NOT NULL AND `{col}` != ''"
        distinct_count = self.execute_query(distinct_count)[0][0]
        
        total_length = f"SELECT SUM(LENGTH(`{col}`)) FROM `{table}` WHERE `{col}` IS NOT NULL AND `{col}` != ''"
        total_length = self.execute_query(total_length)[0][0]
        avg_length = total_length / total_count
        
        if 'name' not in col.lower() and avg_length > 50:
            return None
        
        compression_ratio = 1 - distinct_count / total_count
        
        if compression_ratio < 0.1 and total_count > 1000:
            return None
        
        query = f"SELECT DISTINCT `{col}` FROM `{table}` WHERE `{col}` IS NOT NULL AND `{col}` != ''"
        rows = self.execute_query(query)
        
        if is_date(rows[0][0]) or is_time(rows[0][0]) or is_number(rows[0][0]):
            # if ratio < 0.5:
            #     print(f"{table}.{col}: ", rows[0][0], ratio)
            return None
        # print(rows[0][0])

        distinct_val_set = set()   
        for row in rows:
            if row[0] is not None:
                distinct_val_set.add(row[0])
        print(f"""
            {table}.{col} has special value: {list(distinct_val_set)[:10]},
            compression_ratio: {compression_ratio},
            distinct_count: {distinct_count},
            total_count: {total_count}
        """)
        return distinct_val_set
    
    # TODO: [Persistence Support] This should be preprocessing part. Too time-consuming for large databases...
    def load_database(self) -> bool:
        dataset_name = encode_string(self.dataset)
        db_id = encode_string(self.db_id)
        _data_split = encode_string(self.data_split)
        schema_path = Path(db_cache_dir)/f"{dataset_name}"/f"{_data_split}"/f"{db_id}"/"schema.pickle"
        schema, res = load_data(schema_path)
        if res:
            self.schema = schema
        return res
    
    def save_database(self) -> None:
        dataset_name = encode_string(self.dataset)
        db_id = encode_string(self.db_id)
        _data_split = encode_string(self.data_split)
        schema_path = Path(db_cache_dir)/f"{dataset_name}"/f"{_data_split}"/f"{db_id}"/"schema.pickle"
        save_data(schema_path, self.schema)
    
    def add_distinct_value(self) -> None:
        global distinct_sum
        for table in self.schema.keys():
            # total_distinct_number = 0
            # for col in self.schema[table].keys():
            #     query = f"SELECT COUNT(DISTINCT `{col}`) FROM `{table}` WHERE `{col}` IS NOT NULL AND `{col}` != ''"
            #     distinct_number = self.execute_query(query)[0][0]
            #     total_distinct_number += distinct_number
            for col in self.schema[table].keys():
                if self.schema[table][col]['type'] == 'TEXT':   # only judge when col is TEXT
                    # self.schema[table][col]['distinct_val'] = self.get_distinct_value(table, col, total_distinct_number)
                    # self.schema[table][col]['distinct_val'] = self.get_distinct_value(table, col)
                    self.schema[table][col]['distinct_val'] = self.get_distinct_value_chess(table, col)
                else:
                    self.schema[table][col]['distinct_val'] = None
        ...
        
    def init_orderable(self) -> None:
        for table in self.schema.keys():
            for column in self.schema[table].keys():
                if self.schema[table][column]['type'] != 'TEXT':
                    continue
                try:
                    if column == 'start_date':
                        ...
                    self.schema[table][column]['orderable'] = _is_orderable(self.db_id, table, column)
                except Exception as e:
                    self.schema[table][column]['orderable'] = False
    
    def is_orderable(self, table:str, column:str) -> bool:
        """
        Whether table.column is orderable.
        
        **type of table.column must be 'TEXT', or return True!**
        Args:
            table (str): case insensitive
            column (str): cast insensitive
        Returns:

        """
        column_info = self.get_column_info(table, column)
        if column_info['type'] != 'TEXT':
            return True
        return column_info['orderable']
    
    # implemented in get_distinct_value()
    def exist_spec_val(self, table:str, col:str) -> bool:
        column_info = self.get_column_info(table, col)
        return column_info['distinct_val'] is not None
    
    def exist_spec_val_description(self, table:str, col:str) -> bool:
        column_info = self.get_column_info(table, col)
        return column_info['description'] != col
        
    def find_all_not_described_spec_val(self) -> None:
        for table in self.schema.keys():
            for col in self.schema[table].keys():
                if self.schema[table][col]['type'] == 'TEXT':
                    if self.exist_spec_val(table, col):
                        if self.schema[table][col]['description'] == col:
                            # not described
                            print(f'{table}.{col} has special value but not described')
                            
    def execution_match(self, sql:str, gold_sql:str, force=False) -> tuple[int, str]:
        if not evaluation and not force:
            return 0, ''
        try:
            gold_result = self.execute_query(gold_sql)
        except FunctionTimedOut as fto:
            return 0, 'timeout'
        except BaseException as be:
            return 0, str(be)
        res, result = 0, None
        try:
            pred_result = self.execute_query(sql)
            # todo: this should permute column order!
            if set(pred_result) == set(gold_result):
                res = 1
        except sqlite3.OperationalError as oe:
            result = str(oe)
        except FunctionTimedOut as fto:
            result = 'timeout'
        except BaseException as be:
            result = str(be)
        return res, result
    
    # # backup
    # def _execution_match(self, sql:str, gold_sql:str) -> tuple[int, str]:
    #     global evaluation
    #     if evaluation:
    #         res, errmsg = _execute_model(sql, gold_sql, self.db_path, SQL_EXEC_TIMEOUT)
    #         return res, errmsg
    #     else:
    #         return 0, ""    # meaningless value!
    
    def get_columns_from_table(self, table_name:str) -> dict:
        """
        get columns from table_name
        
        Args:
            table_name (str): table name, case insensitive!
        Returns:
            columns (dict): columns in the table, schema[table_name]
        """
        for table in self.schema.keys():
            if table.lower() == table_name.lower():
                return self.schema[table]
        raise NoSuchTableError
    
    def get_column_info(self, table_name:str, column_name:str):
        """
        get column info
        
        Args:
            table_name (str): table name, case insensitive!
            column_name (str): column name, case insensitive!
        Returns:
            column_info (dict): column info, schema[table_name][column_name]
        """
        columns = self.get_columns_from_table(table_name)
        for column in columns.keys():
            if column.lower() == column_name.lower():
                return columns[column]
        raise NoSuchColumnError
    
    def column_in_table(self, table_name:str, column_name:str) -> bool:
        """
        Whether column_name in table_name
        
        Args:
            table_name (str): case insensitive
            column_name (str): case insensitive
        Returns:
            bool
        """
        try:
            self.get_column_info(table_name, column_name)
            return True
        except NoSuchColumnError:
            return False

    def get_fk_from_table(self, table_name:str) -> dict:
        """
        get keys from table_name
        
        Args:
            table_name (str): table name, case insensitive!
        Returns:
            keys (dict): keys in the table, pkfk[table_name]
        """
        for table in self.pkfk['fk_dict'].keys():
            if table.lower() == table_name.lower():
                return self.pkfk['fk_dict'][table]
        raise NoSuchTableError
    
    def get_pk_from_table(self, table_name:str) -> list:
        """
        get primary keys from table_name
        
        Args:
            table_name (str): table name, case insensitive!
        Returns:
            primary_keys (list): primary keys in the table, pkfk[table_name]
        """
        for table in self.pkfk.keys():
            if table.lower() == table_name.lower():
                return self.pkfk['pk_dict'][table]
        raise NoSuchTableError
        
    def init_disjoint_set(self) -> None:
        assert self.schema is not None and self.pkfk is not None
        assert self.pkfk != {} and self.schema != {}
        tbl_col_list = list()
        for tbl_name in self.schema.keys():
            for col_name in self.schema[tbl_name].keys():
                tbl_col_list.append(TableColumnPair(tbl_name, col_name))
                
        self.disjoint_set = DSU(tbl_col_list)
        
        for from_tbl in self.pkfk['fk_dict'].keys():
            for fk in self.get_fk_from_table(from_tbl):
                self.disjoint_set.union(
                    TableColumnPair(from_tbl, fk['from_col']), 
                    TableColumnPair(fk['to_table'], fk['to_col'])
                )
            
    def column_exist_fk_relationship(self, table1:str, col1:str, table2:str, col2:str) -> bool:
        """
        Whether table1.col1 and table2.col2 have foreign key relationship
            -> iff exist Path which start from table1.col1 and end in table2.col2
        
        Args:
            all case insensitive
        """
        return self.disjoint_set.same(TableColumnPair(table1, col1), TableColumnPair(table2, col2))
    
    def _get_fk_relationship(self, current_tbl:str, target_tbl:str, current_path:List, fk_paths:List, visited:List[str]) -> None:
        ...
        visited.append(current_tbl)
        if current_tbl.lower() == target_tbl.lower():
            fk_paths.append(deepcopy(current_path))
            visited.pop()
            return
            
        # refering
        for fk in self.get_fk_from_table(current_tbl):
            from_col = fk['from_col']
            to_tbl = fk['to_table']
            to_col = fk['to_col']
            if to_tbl in visited:
                continue

            current_path.append({
                "T1": current_tbl, "C1": from_col,
                "T2": to_tbl, "C2": to_col
            })
            self._get_fk_relationship(to_tbl, target_tbl, current_path, fk_paths, visited)
            current_path.pop()
            
        # refered
        for tbl in self.schema.keys():
            if tbl.lower() == current_tbl.lower():
                continue
        
            for fk in self.get_fk_from_table(tbl):
                if tbl in visited:
                    continue
                if fk['to_table'].lower() == current_tbl.lower():
                    current_path.append({
                        "T1": current_tbl, "C1": fk['to_col'],
                        "T2": tbl, "C2": fk['from_col']
                    })
                    self._get_fk_relationship(tbl, target_tbl, current_path, fk_paths, visited)
                    current_path.pop()
                
            
        visited.pop()
                
    def get_fk_relationship(self, table1:str, table2:str) -> List[List[Dict[str, str]]]:
        """
        
        Args:
            table1 (str): case insensitive
            table2 (str): case insensitive
        
        Returns:
            def := Path_i_j = { "T1":str, "C1":str, "T2":str, "C2":str }
            
            def := Path_i = [Path_i_1, Path_i_2, ..., Path_i_j]
            Path_i_j in Path_i should have order!
                -> Path_i_k["T2"] == Path_i_(k+1)["T1"]
            
            ```
            [Path_1, Path_2, ..., Path_i]
            ```
        """
        fk_paths:List[List[Dict[str, str]]] = []
        current_path: List[Dict[str, str]] = []
        visited: List[str] = list()
        visited.append(table1)
        self._get_fk_relationship(table1, table2, current_path, fk_paths, visited)
        
        for path in fk_paths:
            for i in range(len(path)-1):
                assert path[i]["T2"] == path[i+1]["T1"]
        
        return deepcopy(fk_paths)

    def init_fk_relationship(self) -> None:
        # iterate all table pairs
        self.fk_relationship:Dict[str, Dict[str, List]] = {}
        for table1 in self.schema.keys():
            for table2 in self.schema.keys():
                if table1 == table2:
                    continue
                fk_paths = self.get_fk_relationship(table1, table2)
                if len(fk_paths) > 0:
                    if table1 not in self.fk_relationship:
                        self.fk_relationship[table1] = {}
                    if table2 not in self.fk_relationship[table1]:
                        self.fk_relationship[table1][table2] = []
                    self.fk_relationship[table1][table2] = fk_paths

def partial_init_database(db_id:str) -> Database:
    # try:
    #     db = Database(db_id, dataset, data_split)
    # except BaseException as be:
    #     traceback.print_exc()
    
    db = Database(db_id, dataset, data_split)
    
    return db

def init_db_schema_prompt(_dataset:str, db_path:str, db_id:str) -> Tuple[str, str]:
    return Schema_Prompt_Builder(_dataset=_dataset, _data_split=data_split, db_path=db_path, db_id=db_id).get_schema_str()

DBs = {}
def init_DBs(db_list:List[str]=None):
    global DBs_name
    
    if db_list:
        DBs_name = db_list
        
    if parallel_init:
        with ProcessPoolExecutor(max_workers=16) as executor:
            future_to_db_id = {executor.submit(partial_init_database, db_id): db_id for db_id in DBs_name}
            for future in as_completed(future_to_db_id):
                try:
                    db_id = future_to_db_id[future]
                    DBs[db_id] = future.result()
                except Exception as e:
                    print(f"Error initializing DB {db_id}: {e}")
                    
        with ProcessPoolExecutor(max_workers=16) as executor:
            future_to_db_id = {executor.submit(init_db_schema_prompt, DBs[db_id].dataset, DBs[db_id].db_path, db_id): db_id for db_id in DBs_name}
            for future in as_completed(future_to_db_id):
                try:
                    db_id = future_to_db_id[future]
                    DBs[db_id].schema_prompt, DBs[db_id].fk_prompt = future.result()
                except Exception as e:
                    print(f"Error initializing schema prompt {db_id}: {e}")
            
    for name in DBs_name:
        if not parallel_init:
            DBs[name] = partial_init_database(name)
            db:Database = DBs[name]
            DBs[name].schema_prompt, DBs[name].fk_prompt = init_db_schema_prompt(dataset_name=db.dataset, db_path=db.db_path, db_id=name)
        else:
            db:Database = DBs[name]
        db.init_vecDB()
        # db.delete_column_desc_collection()
        # db.delete_distinct_val_collection()
        # if db.conn is not None:
        #     db.conn.close()
        #     db.conn = None
                
    for db in DBs.values():
        print(f'\33[32m{db.db_id}\33[0m')
        
# # You must call init_DBs() at your first time reference DBs!
# init_DBs()

if __name__ == '__main__':
    init_DBs()
    
    # distinct_col = set()
    # for db_id in DBs_name:
    #     db:Database = DBs[db_id]
    #     for table in db.schema.keys():
    #         for col in db.schema[table].keys():
    #             if db.schema[table][col]['distinct_val']:
    #                 distinct_col.add(f"{table}.{col}")
    # print(repr(distinct_col))
    # print(len(distinct_col))