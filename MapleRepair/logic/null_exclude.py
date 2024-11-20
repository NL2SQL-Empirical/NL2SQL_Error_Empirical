import sqlglot
import sqlglot.expressions
import sqlglot.optimizer
from sqlglot.optimizer.scope import build_scope
from sqlglot.optimizer.scope import find_all_in_scope
from MapleRepair.repairer_base import RepairerBase
from MapleRepair.Database import Database, DBs
from MapleRepair.SQL import SQL
from MapleRepair.utils.format import *
from MapleRepair.config import *
from copy import deepcopy
from typing import Tuple
from MapleRepair.utils.sqlite_dialect import SQLite_Dialects


def extract_min_agg_columns(query:sqlglot.optimizer.Scope) -> list[sqlglot.exp.Column]:

    min_agg_columns = []
    try:
        for min_agg in find_all_in_scope(query.expression, sqlglot.exp.Min):
            if isinstance(min_agg.this, sqlglot.exp.Column):
                min_agg_columns.append(min_agg.this)
            
    except Exception as e:
        pass
        # print(f'Error: {e}')
    return min_agg_columns

def extract_orderby_columns(query:sqlglot.optimizer.Scope) -> list[sqlglot.exp.Column]:
    orderby_columns = []
    try:
        for orderby in find_all_in_scope(query.expression, sqlglot.exp.Order):
            for expression in orderby.expressions:
                if isinstance(expression.this, sqlglot.exp.Column) and expression.args['desc'] == False:
                    orderby_columns.append(expression.this)

    except Exception as e:
        pass
        # print(f'Error: {e}')
    return orderby_columns
    
def generate_is_not_null_clause(table: str, col: str):
    '''
    Brief:
        Generate the SQL clause for checking the column is not null
        
    Args:
        table (str): the table name
        col (str): the column name
    '''
    if table == '':
        raise ValueError('Table name is empty, Make sure the column is properly qualified')
    return f'`{table}`.`{col}` IS NOT NULL'

    
def add_not_null_exclude(sql:sqlglot.Expression, db_id:str) -> Tuple[str, bool]:
    '''
    Brief:
        Add the NOT NULL clause to Null Value sensitive SQL
        
    Args:
        sql (str): the SQL statement
        db_id (str): the database id
        exclued_selected_colomn (bool): whether to exclude the selected columns
        exclude_ordered_colume (bool): whether to exclude the ordered columns
        
    Returns:
        str: the new SQL statement with potential NOT NULL clauses
        bool: whether the NOT NULL clause is added
    '''
    
    detected:bool = False
    may_detect:bool = False
    original_sql = deepcopy(sql)
    
    root = build_scope(sql)
    for scope in root.traverse():
        where_clause = []
        
        node = scope.expression
        # extract the columns in the MIN aggregation
        min_agg_columns = extract_min_agg_columns(scope)
        # extract the columns in the ORDER BY clause
        orderby_columns = extract_orderby_columns(scope)
        
        for col in min_agg_columns:
            where_clause.append(generate_is_not_null_clause(col.table, col.name))
                
        for col in orderby_columns:
            where_clause.append(generate_is_not_null_clause(col.table, col.name))

        where_clause = list(set(where_clause))
        
        if where_clause:
            may_detect = True
            
        for clause in where_clause:
            # print(f'clause: {clause}')
            node = node.where(clause, copy=False, dialect=SQLite_Dialects)
            
    if not may_detect:
        return None, False
    
    #TODO: Check WHERE Condition instead of real execution     
    #NOTE: Check the execution result(Optional)
    db:Database = DBs[db_id]
    matched, errmsg = db.execution_match(sql.sql(dialect=SQLite_Dialects), original_sql.sql(dialect=SQLite_Dialects), True)
    if matched == 0:
        detected = True
        
    return sql.sql(dialect=SQLite_Dialects), detected
    
class Null_Value_Repairer(RepairerBase):
    """
        target to Text-to-SQL error: **Ascending Sort with NULL Value**
    """
    def __init__(self):
        super().__init__()
        self.cache_sql:str = None
        
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> Tuple[SQL, int]:
        before = sql.statement
        corrected_sql = self.cache_sql
        
        # try:
        #     corrected_sql, _ = add_not_null_exclude(sql.parsed, db_id)
        # except Exception as e:
        #     self.exception_case.append((sql, db_id, e))
        #     self.logging(sql.question_id, before, sql.statement, False, {"Exception": str(e)})
        #     return sql, originalres
        
        res, errmsg = DBs[db_id].execution_match(corrected_sql, gold_sql)
        sql.update(corrected_sql, "Null_Value_Repairer", errmsg)
        
        self.logging(sql.question_id, before, sql.statement, False, {})
        self.repair_update(sql, gold_sql, db_id, originalres, res)
        return sql, res
    
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        '''
        *** Need Clarification ***
        Not indicator for the error for null value error
        '''
        self.cache_sql = None
        try:
            self.cache_sql, res = add_not_null_exclude(sql.parsed.copy(), sql.db_id)
        except Exception as e:
            self.exception_case.append((sql, db_id, e))
            return False
        self.detect_update(sql, gold_sql, db_id, res, originalres)
        return res


