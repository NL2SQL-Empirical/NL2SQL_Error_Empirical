import sqlglot
from sqlglot.expressions import cast
import sqlglot.expressions
from MapleRepair.repairer_base import RepairerBase
from MapleRepair.SQL import SQL
from MapleRepair.Database import DBs, Database
from MapleRepair.utils.sqlite_dialect import SQLite_Dialects
from copy import deepcopy
from typing import Tuple

def cast_repair(sql:SQL) -> Tuple[str, bool]:
    """
    Returns:
        str: repaired sql if detected else original sql
        bool: Whether given sql is detected.
    """
    db:Database = DBs[sql.db_id]
    _sql = deepcopy(sql)
    
    def valid_div(left, right):        
        for operand in (left, right):
            if isinstance(operand, sqlglot.expressions.Paren):
                operand = operand.this
                
            if isinstance(operand, sqlglot.expressions.Cast):
                return True
            elif isinstance(operand, sqlglot.expressions.Column):
                table = operand.args['table'].args['reference_table_name']
                column = operand.args['this'].this
                column_info = db.get_column_info(table, column)
                column_type = column_info['type'].upper()
                if column_type in ("FLOAT", "REAL"):
                    return True
                elif column_type in ("INT", "INTEGER"):
                    pass
                else:
                    pass
            elif isinstance(operand, sqlglot.expressions.Mul):
                operand_left = operand.left
                operand_right = operand.right
                for sub_operand in (operand_left, operand_right):
                    if isinstance(sub_operand, sqlglot.expressions.Literal) and '.' in sub_operand.this:
                        return True
            else:
                pass
        return False
    
    for div_node in _sql.parsed.find_all(sqlglot.expressions.Div):
        left = div_node.left
        right = div_node.right
    
        if valid_div(left, right):
            continue
            
        cast_node = cast(div_node.left, sqlglot.expressions.DataType.build('float', 'sqlite'))
        div_node.left.replace(cast_node)
        same, errmsg = db.execution_match(sql.parsed.sql(dialect=SQLite_Dialects), _sql.parsed.sql(dialect=SQLite_Dialects), True)
        if not same:
            return _sql.parsed.sql(dialect=SQLite_Dialects), True
            
    return sql.parsed.sql(dialect=SQLite_Dialects), False

class Div_Cast_Repairer(RepairerBase):
    """
        target to Text-to-SQL error: **Missing CAST**
    """
    def __init__(self):
        super().__init__()
    
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        if not sql.unaliased:
            return False
        self.cache_sql, detected = cast_repair(sql)
        self.detect_update(sql, gold_sql, db_id, detected, originalres)
        return detected
        
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        db:Database = DBs[db_id]
        before = sql.statement
        new_sql_statement = self.cache_sql
        
        res, errmsg = db.execution_match(new_sql_statement, gold_sql)
        sql.update(new_sql_statement, "Div_Cast_Repairer", errmsg)
        
        self.logging(sql.question_id, before, sql.statement, False, {})
        self.repair_update(sql, gold_sql, db_id, originalres, res)
        return sql, res