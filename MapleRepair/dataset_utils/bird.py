import csv
from MapleRepair.config import db_root_path
import sqlite3
from MapleRepair.utils.pkfk import get_pkfk_info
from typing import Tuple
from pathlib import Path
import chardet
    
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
            schema[table] = {column[1]: {'type': column[2].upper()} for column in columns }
        
        return schema

class Bird_Initializer():
    def __init__(self, db_id:str) -> None:
        self.db_id = db_id
        
    def do_init(self) -> Tuple[dict, dict]:
        self.pkfk = self._init_pkfk()
        self.schema = self._init_schema()
        self.csv_info = self._read_csv_info(self.db_id)
        self._add_description()
        return self.schema, self.pkfk
    
    def _init_schema(self) -> dict:
        '''
        schema:dict = {
            table_name:str : {
                col_name:str : {
                    type:str,
                    description:str,
                    distinct_val:Optional[set()],
                    date_format: Optional[str]
                }
            }
        }
        '''
        return get_schema(self.db_id)
        
    def _init_pkfk(self) -> dict:
        '''
        pkfk:dict = {
            table_name:str : {
                primarykey:[], 
                foreignkey:str : {
                    # table itself is 'from_table'
                    from_col: str
                    to_table: str
                    to_col: str
                }
            }
        }
        '''
        return get_pkfk(self.db_id)
    
    def _read_csv_info(self, db_id) -> dict:
        
        def detect_encoding(file_path):
            """检测文件的编码"""
            with open(file_path, 'rb') as f:
                raw_data = f.read()
                result = chardet.detect(raw_data)
                return result['encoding']
            
        schema_des = {'database_name': db_id, 'tables': {}}
        for table in self.schema.keys():
            csv_file = Path(db_root_path) / f'{db_id}/database_description/{table}.csv'
            if not csv_file.exists():
                for file_path in csv_file.parent.iterdir():
                    if file_path.stem.lower() == table.lower():
                        csv_file = file_path
            encoding = detect_encoding(csv_file)
            # with open(csv_file, newline='', encoding='utf-8-sig') as csvfile:
            with open(csv_file, newline='', encoding=encoding) as csvfile:
                reader = csv.DictReader(csvfile)
                s = {'table_name': table, 'columns': {}}
                for row in reader:
                    original_column_name = row['original_column_name'].replace('\n', ' ').strip()
                    s['columns'][original_column_name] = {
                        'detailed column name': row['column_name'].replace('\n', ' '),
                        'column_description': row['column_description'].replace('\n', ' '),
                        'data_format': row['data_format'].replace('\n', ' '),
                        'value_description': row['value_description'].replace('\n', ' ') if row['value_description'] is not None else ''
                    }
                schema_des['tables'][table] = s
        return schema_des
    
    def _add_description(self) -> None:
        for table in self.csv_info['tables'].keys():
            for col in self.csv_info['tables'][table]['columns'].keys():
                description = self.csv_info['tables'][table]['columns'][col]['column_description']
                detailed_column_name = self.csv_info['tables'][table]['columns'][col]['detailed column name']
                if col not in self.schema[table].keys():
                    continue
                self.schema[table][col]['description'] = description if description != '' else detailed_column_name
                # make sure the description is not empty, if empty, use the detailed column name.
                if self.schema[table][col]['description'] == '':
                    self.schema[table][col]['description'] = col
                assert self.schema[table][col]['description'] != ''
                
