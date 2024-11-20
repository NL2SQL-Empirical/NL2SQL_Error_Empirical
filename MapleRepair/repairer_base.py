from abc import ABC, abstractmethod

import sqlglot.expressions
from MapleRepair.SQL import SQL
import os
from MapleRepair.config import evaluation
import sqlglot.optimizer.scope
from MapleRepair.Database import Database, DBs
from MapleRepair.utils.sqlite_dialect import SQLite_Dialects
from MapleRepair.utils.format import read_json, write_json
from MapleRepair.config import result_root_dir
from typing import List, Dict, Any
from pathlib import Path
import json
from MapleRepair.utils.persistence import make_log

FAKE_REPAIR_DEBUG = False

from enum import Enum
class Table_Identifier_Type(Enum):
    NAME = 1
    ALIAS = 2
    EMPTY = 3

# Decorator
def detect_statistics():
    raise NotImplementedError

# Decorator
def repair_statistics():
    raise NotImplementedError

class RepairerBase(ABC):
    def __init__(self):
        self.false_detecting = []
        self.success_detected = []
        self.false_repairing = []
        self.success_repairing = []
        self.fail_rapairing = []
        self.not_detected = []
        self.exception_case = []
    
    @abstractmethod
    def repair(self, sql:SQL, gold_sql:str, db_id:str, originalres:int) -> tuple[SQL, int]: # return repaired sql and res
        pass
    
    @abstractmethod
    def detect(self, sql:SQL, gold_sql:str, db_id:str, originalres:int) -> bool:
        pass
    
    def detect_update(self, sql:SQL, gold_sql:str, db_id:str, detected:bool, originalres) -> None:
        if detected:
            if originalres == 1:
                self.false_detecting.append((sql.question_id, sql.statement, gold_sql, db_id, originalres))
            else:
                self.success_detected.append((sql.question_id, sql.statement, gold_sql, db_id, originalres))
        else:
            self.not_detected.append((sql.question_id, sql.statement, gold_sql, db_id, originalres))
        
    def repair_update(self, sql:SQL, gold_sql:str, db_id:str, originalres:int, res:int) -> None:
        if res == originalres != 1:
            self.fail_rapairing.append((sql.question_id, sql.statement, gold_sql, db_id, originalres, res))
        if res == 1 and originalres != 1:
            self.success_repairing.append((sql.question_id, sql.statement, gold_sql, db_id, originalres, res))
        if res != 1 and originalres == 1:
            self.false_repairing.append((sql.question_id, sql.statement, gold_sql, db_id, originalres, res))
            
    def logging(self, idx:int, before_sql:str, after_sql:str, call_llm:bool, details:dict) -> None:
        log_path = result_root_dir / "sql_logs" / f"{idx}.json"
        if log_path.exists():
            log:List[Dict] = read_json(log_path)
        else:
            log:List[Dict] = []
        # append log
        this_log = {
            "repairer": self.__class__.__name__,
            "before_sql": before_sql,
            "after_sql": after_sql,
            "call_llm": call_llm,
            "details": details
        }
        log.append(this_log)
        content = json.dumps(log)
        make_log(log_path, content)
            
    def exception_case_update(self, sql:SQL, gold_sql:str, db_id:str, originalres:int):
        self.exception_case.append((sql, gold_sql, db_id, originalres))
        
    def print_fail_repairing_log(self):
        for fail_case in self.fail_rapairing:
            fail_case[0].print_repair_log()
        
    def fake_repair(self, sql:SQL, scope:sqlglot.optimizer.scope.Scope, error_tc:dict) -> SQL:
        """
            This function is design to remove the error by randomly replace
            the error part with the one without error.
            So that, we will not loop in the same one error !!!
            
            This function can only fake repair simple table-column mismatch!!!
            
            Args:
                sql (SQL): 
                scope (Scope):
                
            Returns:
                SQL: **modified** scope in Args.
        """
        try:
            db:Database = DBs[sql.db_id]
            column_exp:sqlglot.expressions.Column = self.get_error_column_expr(scope, error_tc)
            join_parent = column_exp.find_ancestor(sqlglot.expressions.Join)
            if join_parent is not None:
                # Column is used as part of ON condition, carefully replace!
                table_alias_or_name = column_exp.args['table'].alias_or_name
                table_name = scope.sources[table_alias_or_name].this.this
                new_column:str = list(db.get_columns_from_table(table_name).keys())[0]
                if FAKE_REPAIR_DEBUG:
                    print(sql.parsed.sql(dialect=SQLite_Dialects))
                    print(error_tc)
                sql.make_fake_replace(
                    sqlglot.expressions.column(new_column, table_alias_or_name),
                    column_exp
                )
                
                sql.partial_update()
                if FAKE_REPAIR_DEBUG:
                    print(sql.parsed.sql(dialect=SQLite_Dialects))
                ...
            else:
                for alias_or_name, expr in scope.sources.items():
                    if isinstance(expr, sqlglot.expressions.Table):
                        new_table_identifier = alias_or_name
                        new_table = expr.name
                new_column:str = list(db.get_columns_from_table(new_table).keys())[0]
                if FAKE_REPAIR_DEBUG:
                    print(sql.parsed.sql(dialect=SQLite_Dialects))
                    print(error_tc)
                sql.make_fake_replace(
                    sqlglot.expressions.column(new_column, new_table_identifier),
                    column_exp
                )
                
                sql.partial_update()
                if FAKE_REPAIR_DEBUG:
                    print(sql.parsed.sql(dialect=SQLite_Dialects))
                ...
        except BaseException as be:
            ...
        return sql
    
    def get_error_column_expr(self, scope:sqlglot.optimizer.scope.Scope, error_tc:dict) -> sqlglot.expressions.Column:
        for column in scope.expression.find_all(sqlglot.expressions.Column):
            if column.args['this'].this == error_tc['column_name']:
                if self.table_identifier_type == Table_Identifier_Type.EMPTY:
                    return column
                elif self.table_identifier_type in (Table_Identifier_Type.NAME, Table_Identifier_Type.ALIAS):
                    if 'table' in column.args and column.args['table'].this == error_tc['table_identifier']:
                        return column
        ...
        