# TODO: Refactoring

import re
import sqlite3
from sqlite3 import OperationalError
from pathlib import Path
from MapleRepair.config import db_root_path

SQL_TEMPLATE = "SELECT DISTINCT `{column}` FROM `{table}` WHERE `{column}` IS NOT NULL ORDER BY `{column}` DESC;"

def execute_sql(db_path: str, sql: str):
    try:
        with sqlite3.connect(db_path, isolation_level=None) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return cursor.fetchall()
    except OperationalError as oe:
        ...
    except Exception as e:
        ...

def contain_number(string) -> bool:
    return bool(re.search(r'\d', string))

def generate_format_from_value(value_str):
    result = re.sub(r'\d', 'x', value_str)
    return result

def format_to_regex(format_string):
    regex_pattern = re.sub(r'x', r'\\d', format_string)
    regex_pattern = f'^{regex_pattern}$'
    return regex_pattern

def detect(input_string, format_string) -> bool:
    pattern = format_to_regex(format_string)
    if re.match(pattern, input_string):
        return False, None    # clear
    else:
        return True, input_string     # may have problem

def _is_orderable(db_id:str, table:str, column:str) -> bool:
    db_path = Path(db_root_path) / db_id / f"{db_id}.sqlite"
    
    sql_t = SQL_TEMPLATE.format(column=column, table=table)
    res = execute_sql(db_path, sql_t)
    
    if not contain_number(res[0][0]):
        # whether order by alphabet?
        return False
    
    format = generate_format_from_value(res[0][0])
    # print(f"format: {res[0][0]} -> {format}")
    detected = False
    special_val = None
    for s in reversed(res):
        detected, special_val = detect(s[0], format)
        if detected:
            return False
        
    return True
