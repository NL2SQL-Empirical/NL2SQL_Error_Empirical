import sqlite3
from pathlib import Path

def get_foreign_keys(database_path, pk_dict):
    """
    dataset indepandent
    """
    with sqlite3.connect(database_path, isolation_level=None) as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = cursor.fetchall()
        
        foreign_keys = {}
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f"PRAGMA foreign_key_list(`{table_name}`);")
            fk_info = cursor.fetchall()
            
            foreign_keys[table_name] = fk_info
        
    res = {}
    for table in foreign_keys:
        res[table] = []
        for fk in foreign_keys[table]:
            if fk[4] is None:
                to_table = fk[2]
                if len(pk_dict[to_table]) == 1:
                    to_col = pk_dict[to_table][0]
                elif len(pk_dict[to_table]) > 1:
                    for pk_col in pk_dict[to_table]:
                        if pk_col.lower() == fk[3].lower():
                            to_col = pk_col
                            break
            else:
                to_col = fk[4]
            fk_item = {                 # (from_col, to_table, to_col) 
                "from_col": fk[3],
                "to_table": fk[2],
                "to_col":   to_col
            }
            assert fk[3] and fk[2] and to_col
            res[table].append(fk_item)  
            
    return res

def get_primary_keys(database_path):
    """
    dataset indepandent
    """
    with sqlite3.connect(database_path, isolation_level=None) as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = cursor.fetchall()
        
        primary_keys = {}
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f"PRAGMA table_info(`{table_name}`);")
            columns_info = cursor.fetchall()
            
            pk_columns = [col[1] for col in columns_info if col[5] != 0]
            
            primary_keys[table_name] = pk_columns
        
        return primary_keys

def get_pkfk_info(db_path:Path):
    """
    dataset indepandent
    """
    pk_dict = get_primary_keys(db_path)
    fk_dict = get_foreign_keys(db_path, pk_dict)
    result = {
        'pk_dict': pk_dict,
        'fk_dict': fk_dict,
    }
    return result

if __name__ == '__main__':
    a = get_pkfk_info('data/bird/train_sampled/train_databases/software_company/software_company.sqlite')
    ...