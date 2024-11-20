import json
import sqlparse
from typing import List

def read_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)
    
def write_json(file_path, data):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)
        
def reorder_dict(d:dict, desired_order:List[str]) -> dict:
    """
    This function will reorder dict as desired_order.
    
    This function will **RECONSTRUCT** the dict!
    
    Args:
        d (dict):
        desired_order (List[str]):
    Returns:
        reordered_dict (dict): `id(d) != id(reordered_dict)`
    """
    ordered_dict = {key: d[key] for key in desired_order}
    return ordered_dict
        
def format_sql(sql_statement):
    formatted_sql = sqlparse.format(sql_statement, reindent=True, keyword_case='upper')
    return formatted_sql

def format_json(json_data):
    formatted_json = json.dumps(json_data, indent=4)
    return formatted_json