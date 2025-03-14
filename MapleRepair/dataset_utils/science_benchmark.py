import argparse
import json
from pathlib import Path
from typing import Tuple
from MapleRepair.config import db_root_path
from MapleRepair.utils.pkfk import get_pkfk_info
import sqlite3

def get_pkfk(db_id:str) -> dict:
    db_path = Path(db_root_path) / f"{db_id}/{db_id}.sqlite"
    pkfk_info = get_pkfk_info(db_path)
    return pkfk_info

def get_schema(db_id:str):
    with sqlite3.connect(Path(db_root_path) / f'{db_id}/{db_id}.sqlite', isolation_level=None) as conn:
        cursor = conn.cursor()

        # Get the list of all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = cursor.fetchall()

        schema = {}
        for table in tables:
            table = table[0]
            cursor.execute(f"PRAGMA table_info('{table}')")
            columns = cursor.fetchall()
            # schema[table] = {column[1]: type_mapping.get(column[2].upper()) for column in columns}
            schema[table] = {column[1]: {'type': column[2].split('(')[0].upper()} for column in columns }
        
        return schema
        
class Sciencebenchmark_Initalizer():
    def __init__(self, db_name:str) -> None:
        """
        # brief: initializes the ScientificBenchmark class
            schema_path: Path to the tables.json file
        """
        self.db_id = db_name
        schema_path = Path(db_root_path) / f"{self.db_id}/tables.json"
        with open(schema_path) as f:
            self.table_json = json.load(f)[0] # weird format, but it's a list of one element
            
    def do_init(self) -> Tuple[dict, dict]:
        self.schema = self._init_schema()
        self.pkfk = self._init_pkfk()
        self.table_json = self.parse_table_json()
        self._add_description()
        return self.schema, self.pkfk
    
    def _init_schema(self) -> dict:
        return get_schema(self.db_id)
        
    def _init_pkfk(self) -> dict:
        return get_pkfk(self.db_id)
            
    def parse_table_json(self) -> dict:
        # NOTE: the [column|table]_desc and the corresponding description are the same. 
        table_json:dict = dict()
        
        for table_id, table in enumerate(self.table_json['table_names_original']):
            table_json[table] = {}
            for idx, (col_idx, col_name) in enumerate(self.table_json['column_names_original']):
                if col_idx == table_id:
                    table_json[table][col_name] = {
                        'description': self.table_json['column_names'][idx][1], # column name
                    }
        return table_json
    
    def _add_description(self) -> None:
        for table in self.schema.keys():
            for col in self.schema[table].keys():
                if col not in self.table_json[table]:
                    self.schema[table][col]['description'] = col
                    continue
                description = self.table_json[table][col]['description']
                # ...
                if col not in self.schema[table].keys():
                    continue
                self.schema[table][col]['description'] = description
                # make sure the description is not empty, if empty, use the detailed column name.
                if self.schema[table][col]['description'] == '':
                    self.schema[table][col]['description'] = col
                assert self.schema[table][col]['description'] != ''

if __name__ == '__main__':
    """
    Usage:
        python schema.py --db_name cordis
    """
    import shutil, pprint, json
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--db_name', default = 'cordis', type=str)
    args = parser.parse_args()
    
    sci_benchmark = Sciencebenchmark_Initalizer(args.db_name)
    
    sci_benchmark.do_init()