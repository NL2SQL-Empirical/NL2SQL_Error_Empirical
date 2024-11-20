import re
from typing import Optional, Set, List, Tuple

import sqlglot.optimizer
import sqlglot.optimizer.scope

from MapleRepair.Customized_Exception import DispatchError, NoForeignKeyError, NoAliasError, NoSuchColumnError

import sqlglot.expressions
from MapleRepair.utils.edit_distance import minDistance
import sqlglot
from MapleRepair.Database import Database
import sqlite3
from MapleRepair.repairer_base import RepairerBase
from MapleRepair.SQL import SQL
from MapleRepair.utils.sqlite_dialect import SQLite_Dialects
from MapleRepair.Database import DBs
from MapleRepair.repairer_prompt import ON_TableColumn_Mismatch_Prompt

def mapping_alias2name(alias:str, scope:sqlglot.optimizer.scope.Scope) -> str:
    """
    Args:
        alias (str): case insensitive
    """
    for alias_or_name, expr in scope.sources.items():
        if alias.lower() == alias_or_name.lower():
            return expr.name
    raise DispatchError("No such alias in the sql.")

def mapping_name2alias(table_name:str, scope:sqlglot.optimizer.scope.Scope) -> str:
    """
    Args:
        table_name (str): case insensitive
    """
    table_exist_flag = False
    return_alias = []
    for alias_or_name, expr in scope.sources.items():
        if isinstance(expr, sqlglot.expressions.Table):
            if expr.name.lower() == table_name.lower():
                table_exist_flag = True
                if expr.alias != '':
                    assert expr.alias == alias_or_name, "expr.alias != alias_or_name"
                    return_alias.append(alias_or_name)
    if len(return_alias) == 1:
        return return_alias[0]
    elif len(return_alias) == 0:
        if table_exist_flag:
            raise NoAliasError("This table has no alias.")
        else:
            raise Exception("No such name in the sql.")
    else:
        assert len(return_alias) > 1
        assert False, "more than 1 alias???"
        
def execute_sql(sql, db_id) -> Optional[str]:
    db:Database = DBs[db_id]
    ret = None
    try:
        db.is_executable(sql)
    except sqlite3.OperationalError as oe:
        ret = oe.args[0]
    return ret

def extract_column_from_errmsg(errmsg:str) -> str:
    # no such column: schools.School Name
    match = re.search(r'no such column: (.*)', errmsg)
    if match:
        return match.group(1)
    else:
        return None
    
def split_tableIdentifier_columnIdentifier(s:str) -> tuple[Optional[str], str]:
    # schools.School Name -> (schools, School Name)
    f = s.split('.')
    if len(f) == 2:
        return f[0], f[1]
    elif len(f) == 1:
        return None, f[0]
    else:
        assert False

EDIT_DISTANCE_THRESHOLD = 3

def is_c_in_t(column:str, table:str, db_id:str) -> bool:
    """
    return whether column in table
    Args:
        column: column name, case insensitive
        table: table name, case insensitive
        db_id: database id
    """
    assert table != '', "table_identifier_type != Table_Identifier_Type.EMPTY"
    db:Database = DBs[db_id]
    return db.column_in_table(table, column)

def get_tables_from_scope(scope:sqlglot.optimizer.scope.Scope) -> List[sqlglot.expressions.Table]:
    tables = []
    for alias_or_name, table_expr in scope.sources.items():
        if isinstance(table_expr, sqlglot.expressions.Table):
            tables.append(table_expr)
    return tables

def get_all_table_names(tables:List[sqlglot.expressions.Table]) -> List[str]:
    table_names = []
    for table in tables:
        table_names.append(table.name)
    return table_names
    
def get_all_table_aliases(tables:List[sqlglot.expressions.Table]) -> List[str]:
    table_aliases = []
    for table in tables:
        if table.alias:
            table_aliases.append(table.alias)
    return table_aliases

from enum import Enum
from MapleRepair.repairer_base import Table_Identifier_Type
    
class Error_Type(Enum):
    SPELL_ERROR = 1
    COLUMN_HALLUCINATION = 2
    
    MISSING_JOIN = 3
    TABLE_COLUMN_MISMATCH = 4
    ALIAS_NOT_USE = 5
    TABLE_COLUMN_MISMATCH_AND_MISSING_JOIN = 6
    IDENTIFIER_HALLUCINATION = 7

class No_Such_Column_Repairer(RepairerBase):
    def __init__(self):
        super().__init__()
        self.error_type = None
        self.table_identifier_type: Table_Identifier_Type = None
        self.candidates = []
        self.target_scope = None
        self.error_tc = {'table_identifier': None, 'table_name':None, 'column_identifier': None, 'column_name': None}
        
        self.cser = Column_Spell_Error_Repairer()
        self.ch  = Hallucination_Repairer()
        self.ih = Identifier_Hallucination_Repairer()
        
        self.mjr = Missing_Join_Repairer()
        self.mtcr = Mismatch_TableColumn_Repairer()
        self.anur = Alias_Not_Use_Repairer()
    
    def locate_error_scope(self, sql:SQL, db_id:str) -> sqlglot.optimizer.scope.Scope:
        target_scope = set()
        root:sqlglot.optimizer.scope.Scope = sql.scope_root
        for scope in root.traverse():
            for column in scope._raw_columns:
                if column.args['this'].this == self.error_tc['column_name']:
                    if self.table_identifier_type == Table_Identifier_Type.EMPTY:
                        error_tc = self._get_error_tc(scope.expression.sql(dialect=SQLite_Dialects), db_id)
                        if error_tc and error_tc['table_identifier'] == self.error_tc['table_identifier'] and \
                            error_tc['column_identifier'] == self.error_tc['column_identifier']:
                            target_scope.add(scope)
                    elif self.table_identifier_type in (Table_Identifier_Type.NAME, Table_Identifier_Type.ALIAS):
                        if 'table' in column.args and column.args['table'].this == self.error_tc['table_identifier']:
                            # FIXME: sometimes, scope error differ from whole sql error if exists multiple errors.
                            error_tc = self._get_error_tc(scope.expression.sql(dialect=SQLite_Dialects), db_id)
                            if error_tc and error_tc['table_identifier'] == self.error_tc['table_identifier'] and \
                               error_tc['column_identifier'] == self.error_tc['column_identifier']:
                                target_scope.add(scope)
                        
        if len(target_scope) > 1:
            # if error happen to be in multiple scopes, just return one of them is ok.
            # remaining scopes will be repaired next turn.
            pass
        if len(target_scope) == 0:
            return None
        return target_scope.pop()

    def _get_error_tc(self, sql:str, db_id:str) -> dict | None:
        """
        only get 'table_identifier', 'column_identifier' and 'column_name' of error_tc  
        `error_tc['column_name'] = error_tc['column_identifier']`  
        **This function will not set self.error_tc!**
        
        Returns:
            if sql is not "no such column" error, return None
        """
        
        # run sql and get error message
        error_tc = {}
        errmsg = execute_sql(sql, db_id)
        if errmsg is None:
            return None
        # parse table.col from error message, here table may be None
        error_tc_str = extract_column_from_errmsg(errmsg)
        if error_tc_str is None:
            return None    # is not "no such column" error
        try:
            error_tc['table_identifier'], error_tc['column_identifier'] = split_tableIdentifier_columnIdentifier(error_tc_str)
            error_tc['column_name'] = error_tc['column_identifier']   # FIXME: assume column without alias
        except:
            return None
        return error_tc

    def error_dispatch(self, sql:SQL, db_id:str=None) -> bool:
        # detect error and return bool
        # detect error and set error_type
        sql_statement = sql.statement
        
        self.error_type = None
        self.table_identifier_type = None
        self.target_scope = None
        self.candidates = []
        self.error_tc = {'table_identifier': None, 'table_name':None, 'column_identifier': None, 'column_name': None}
        
        self.error_tc = self._get_error_tc(sql_statement, db_id)
        if self.error_tc is None:
            return False
        
        # set self.table_identifier_type
        if self.error_tc['table_identifier'] is None:
            self.table_identifier_type = Table_Identifier_Type.EMPTY
        elif self.error_tc['table_identifier'] in DBs[db_id].schema.keys():
            self.table_identifier_type = Table_Identifier_Type.NAME
            self.error_tc['table_name'] = self.error_tc['table_identifier']
        else:
            self.table_identifier_type = Table_Identifier_Type.ALIAS
            
        self.target_scope = self.locate_error_scope(sql, db_id)
        if self.target_scope is None:
            # exception here
            raise NotImplementedError("No Such Column (error dispatch): locate scope fail.")
        
        sql_statement = self.target_scope.expression.sql(dialect=SQLite_Dialects)
        
        # find all tables and cols from sql
        table_exprs_in_scope = get_tables_from_scope(self.target_scope)
        tables_in_scope = get_all_table_names(table_exprs_in_scope)
        
        # deal with IDENTIFIER_HALLUCINATION
        if self.table_identifier_type == Table_Identifier_Type.NAME:
            ...     # This will not be a IDENTIFIER HALLUCINATION
        elif self.table_identifier_type == Table_Identifier_Type.ALIAS:
            hallucination_flag = True
            for table_alias in get_all_table_aliases(table_exprs_in_scope):
                if table_alias.lower() == self.error_tc['table_identifier'].lower():
                    hallucination_flag = False
                    break
            if hallucination_flag:
                self.error_type = Error_Type.IDENTIFIER_HALLUCINATION
                # print(sql)
                # print(self.error_tc)
                ...
                
        if self.error_type == Error_Type.IDENTIFIER_HALLUCINATION:
            # disabled since not mentioned in paper!
            return False
            # return True
            
        if self.table_identifier_type == Table_Identifier_Type.ALIAS:
            try:
                self.error_tc['table_name'] = mapping_alias2name(self.error_tc['table_identifier'], self.target_scope)
            except DispatchError as de:
                assert False, "Should never reach here, ALIAS_HALLUCINATION will go to another branch!"
        
        ED_list = []
        
        for table in DBs[db_id].schema.keys():
            for col in DBs[db_id].schema[table].keys():
                ED_list.append((table, col, minDistance(self.error_tc['column_name'].lower(), col.lower())))
                
        ED_list.sort(key=lambda x: x[2])
        
        if ED_list[0][2] == 0:
            # ED = 0 exists.
            all_in_sql, all_not_in_sql = True, True
            for table, col, ED in ED_list:
                if ED == 0:
                    if table.lower() in map(str.lower, tables_in_scope):
                        all_not_in_sql = False
                    else:
                        all_in_sql = False
                    self.candidates.append((table, col, ED))
            
            if all_in_sql or (not all_in_sql and not all_not_in_sql):
                # all or some corresponding tables are in sql
                if self.table_identifier_type == Table_Identifier_Type.EMPTY:
                    # print(sql)
                    raise DispatchError()  
                if is_c_in_t(self.error_tc['column_name'], self.error_tc['table_name'], db_id):
                    # c in t
                    if self.table_identifier_type != Table_Identifier_Type.NAME:
                        raise DispatchError()
                    self.error_type = Error_Type.ALIAS_NOT_USE
                else:
                    # c not in t
                    self.error_type = Error_Type.TABLE_COLUMN_MISMATCH
            else:
                # all or some corresponding tables are not in sql
                if self.table_identifier_type == Table_Identifier_Type.EMPTY:
                    self.error_type = Error_Type.MISSING_JOIN
                else:
                    if is_c_in_t(self.error_tc['column_name'], self.error_tc['table_name'], db_id):
                        # c in t
                        # assert self.table_identifier_type != Table_Identifier_Type.ALIAS
                        if self.table_identifier_type == Table_Identifier_Type.ALIAS:
                            return False
                        self.error_type = Error_Type.MISSING_JOIN
                    else:
                        # c not in t
                        self.error_type = Error_Type.TABLE_COLUMN_MISMATCH_AND_MISSING_JOIN
        else:
            # There is no corresponding table!!!
            if ED_list[0][2] <= EDIT_DISTANCE_THRESHOLD:
                # ED <= EDIT_DISTANCE_THRESHOLD exists.
                self.error_type = Error_Type.SPELL_ERROR
                for table, col, ED in ED_list:
                    if ED <= EDIT_DISTANCE_THRESHOLD:
                        self.candidates.append((table, col, ED))
            else:
                # ED <= EDIT_DISTANCE_THRESHOLD doesn't exist.
                # Column Hallucination here.
                return False
                self.error_type = Error_Type.COLUMN_HALLUCINATION
        assert self.error_type is not None
        return True
        
    def detect(self, sql:SQL, sql_gold:str, db_id:str, originalres:int) -> bool:
        res = False
        if sql.parsed:
            try:
                res = self.error_dispatch(sql, db_id)
            except DispatchError as e:
                self.exception_case_update(sql, sql_gold, db_id, originalres)
        self.detect_update(sql, sql_gold, db_id, res, originalres)
        return res
    
    def repair(self, sql:SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        try:
            repaired_sql, res = self._repair(sql, gold_sql, db_id, originalres)
            repaired_sql._unalias_check()
            return repaired_sql, res
        except DispatchError as e:
            return sql, originalres
    
    def _repair(self, sql:SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        # according to self.error_type, self.canidates, self.table_identifier_type decide how to repair.
        res = originalres
        cnt = 0
        while self.error_dispatch(sql, db_id):
            cnt += 1
            if cnt > 6:
                return sql, res
            if   self.error_type == Error_Type.SPELL_ERROR:
                self.cser.error_type = self.error_type
                self.cser.candidates = self.candidates
                self.cser.error_tc = self.error_tc
                self.cser.table_identifier_type = self.table_identifier_type
                self.cser.target_scope = self.target_scope
                _ = self.cser.detect(sql, gold_sql, db_id, originalres)
                sql, res = self.cser.repair(sql, gold_sql, db_id, originalres)
            elif self.error_type == Error_Type.COLUMN_HALLUCINATION:
                self.ch.error_tc = self.error_tc
                self.ch.table_identifier_type = self.table_identifier_type
                self.ch.target_scope = self.target_scope
                _ = self.ch.detect(sql, gold_sql, db_id, originalres)
                sql, res = self.ch.repair(sql, gold_sql, db_id, originalres)
            elif self.error_type == Error_Type.TABLE_COLUMN_MISMATCH:
                self.mtcr.error_tc = self.error_tc
                self.mtcr.table_identifier_type = self.table_identifier_type
                self.mtcr.candidates = self.candidates
                self.mtcr.target_scope = self.target_scope
                _ = self.mtcr.detect(sql, gold_sql, db_id, originalres)
                sql, res = self.mtcr.repair(sql, gold_sql, db_id, originalres)
            elif self.error_type == Error_Type.ALIAS_NOT_USE:
                self.anur.error_tc = self.error_tc
                self.anur.table_identifier_type = self.table_identifier_type
                self.anur.target_scope = self.target_scope
                _ = self.anur.detect(sql, gold_sql, db_id, originalres)
                sql, res = self.anur.repair(sql, gold_sql, db_id, originalres)
            elif self.error_type == Error_Type.MISSING_JOIN:
                self.mjr.error_tc = self.error_tc
                self.mjr.table_identifier_type = self.table_identifier_type
                self.mjr.candidates = self.candidates
                self.mjr.target_scope = self.target_scope
                _ = self.mjr.detect(sql, gold_sql, db_id, originalres)
                sql, res =  self.mjr.repair(sql, gold_sql, db_id, originalres)
            elif self.error_type == Error_Type.TABLE_COLUMN_MISMATCH_AND_MISSING_JOIN:
                # Repair Table Column Mismatch first, then Missing JOIN.
                self.mtcr.error_tc = self.error_tc
                self.mtcr.table_identifier_type = self.table_identifier_type
                self.mtcr.candidates = self.candidates
                self.mtcr.target_scope = self.target_scope
                _ = self.mtcr.detect(sql, gold_sql, db_id, originalres)
                sql, res = self.mtcr.repair(sql, gold_sql, db_id, originalres)
            elif self.error_type == Error_Type.IDENTIFIER_HALLUCINATION:
                self.ih.error_tc = self.error_tc
                self.ih.target_scope = self.target_scope
                _ = self.ih.detect(sql, gold_sql, db_id, originalres)
                sql, res = self.ih.repair(sql, gold_sql, db_id, originalres)
            else:
                if self.candidates[0][2] == 0:
                    ...
                else:
                    ...
                break
        # do not sql.update() here since sql has been updated in the process of repair.
        
        # sql.restore_fake_replace()  # debug only!!!
        # sql.partial_update()
        
        if not sql.fake_mapping:
            res, errmsg = DBs[db_id].execution_match(sql.statement, gold_sql)
            self.repair_update(sql, gold_sql, db_id, originalres, res)
        else:
            self.repair_update(sql, gold_sql, db_id, originalres, originalres)
        return sql, res

# Hint: Sperate Machenism and Strategy.
class Missing_Join_Repairer(RepairerBase):
    def __init__(self):
        super().__init__()
        self.table_identifier_type: Table_Identifier_Type = None
        self.error_tc = {'table_identifier': None, 'table_name':None, 'column_identifier': None, 'column_name': None}
        self.candidates = []    # (table, col, ED)
        self.target_scope:sqlglot.optimizer.scope.Scope = None
        
    def insert_join(self, on_condition, table_right_name, table_right_alias, sql:sqlglot.expressions.Select) -> None:
        # if table_right_alias is empty str, there will be no AS.
        # return ... table_left INNER JOIN table_right ON on_condition ...
        # using sqlglot.join, table_left will be automatically selected according to the on_condition.
        
        parsed_expression = sql
        parsed_expression.join(
            expression=table_right_name,
            on=on_condition,
            join_alias=table_right_alias,
            dialect=SQLite_Dialects,
            copy=False
        )
        
    def detect(self, sql: str, gold_sql: str, db_id: str, originalres: int) -> bool:
        detected = True
        self.detect_update(sql, gold_sql, db_id, detected, originalres)
        return detected
        
    def extract_on_condition(self, db_id:str, table_to_be_joined:str, scope:sqlglot.optimizer.scope.Scope) -> Tuple[str, str]:
        """
        Args:
            table_to_be_joined (str): table name, case insensitive.
        """
        db:Database = DBs[db_id]
        
        alias_name_mapping = {}
        all_table_name_in_scope = set()
        for alias_or_name, expr in scope.sources.items():
            if isinstance(expr, sqlglot.expressions.Table):
                all_table_name_in_scope.add(expr.name)
                alias_name_mapping[expr.name] = alias_or_name
        all_table_name_in_scope = list(all_table_name_in_scope)
        
        all_fk_relationships = {}
        for table_name in all_table_name_in_scope:
            if table_name not in all_fk_relationships:
                all_fk_relationships[table_name] = []
            all_fk_relationships[table_name] = db.get_fk_relationship(table_name, table_to_be_joined)
        
        exist_fk:bool = False    
        for table_name, fk_relationships in all_fk_relationships.items():
            if len(fk_relationships):
                exist_fk = True
        
        if not exist_fk:
            raise NoForeignKeyError
        
        # choose one fk (shortest path)
        shortest_path = None
        path_distance = []
        for table_name, paths in all_fk_relationships.items():
            for path in paths:
                path_distance.append((table_name, path, len(path)))
        path_distance = sorted(path_distance, key=lambda x:x[2])
        
        for table, path, dist in path_distance:
            if dist == 0:
                continue
            shortest_path = path
            break
            
        # shortest_path = path_distance[0][1]
        
        if len(shortest_path) > 1:
            # Two cases here:
            #   1. All tables in path can not be removed
            #   2. Some tables in path can be removed and then len(shortest_path) == 1
            
            path_i_th = (shortest_path[0], shortest_path[1])
            while path_i_th[0]['T2'] == path_i_th[1]['T1'] and path_i_th[0]['C2'] == path_i_th[1]['C1']:
                path_i_th[1]['T1'], path_i_th[1]['C1'] = path_i_th[0]['T1'], path_i_th[0]['C1']
                shortest_path = shortest_path[1:]
                if len(shortest_path) > 1:
                    path_i_th = (shortest_path[0], shortest_path[1])
                else:
                    break
                
            if len(shortest_path) == 1:
                # For case 2.
                pass
            else:
                # For case 1.
                # when a table in shortest path does not exist in this SQL query
                # join this table first, then join the rest tables next turn.
                # to do so, we have to change `table_right_name` outside, just return to it.
                raise NotImplementedError("At least two tables to be joined and can not be compressed.")
        
        # synthesis it to condition string
        path_i = shortest_path[0]
        table = path_i['T1']
        for k_table in alias_name_mapping.keys():
            if table.lower() == k_table.lower():
                table = k_table
        table_identifier = alias_name_mapping[table]
        on_condition = f"{table_identifier}.{path_i['C1']} = {path_i['T2']}.{path_i['C2']}"
        ...
        
        return on_condition, table_to_be_joined
    
    def table_selector(self) -> str:
        return self.candidates[0][0]
        
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        before = sql.statement
        
        if self.table_identifier_type == Table_Identifier_Type.EMPTY:
            table_right_name = self.table_selector()
        else:
            table_right_name = self.error_tc['table_name']
            
        try:
            on_condition, table_right_name = self.extract_on_condition(db_id, table_right_name, self.target_scope)
        except NoForeignKeyError as e:
            raise NotImplementedError(f"table: {table_right_name} has no fk with existing tables in this SQL query.")
        
        self.insert_join(on_condition, table_right_name, '', self.target_scope.expression)
        
        sql.partial_update()
        res, errmsg = DBs[db_id].execution_match(sql.statement, gold_sql)
        
        self.logging(sql.question_id, before, sql.statement, False, {})
        self.repair_update(sql, gold_sql, db_id, originalres, res)
        return sql, res
        
class Mismatch_TableColumn_Repairer(RepairerBase):
    def __init__(self):
        super().__init__()
        self.otcmr = ON_TableColumn_Mimatch_Repairer()
        
        self.table_identifier_type: Table_Identifier_Type = None
        self.error_tc = {'table_identifier': None, 'table_name':None, 'column_identifier': None, 'column_name': None}
        self.candidates = []
        self.ON_condition_exception = []
        
        # pass by caller
        self.target_scope:sqlglot.optimizer.scope.Scope = None
        
    def detect(self, sql: str, gold_sql: str, db_id: str, originalres: int) -> bool:
        detected = True
        self.detect_update(sql, gold_sql, db_id, detected, originalres)
        return detected
    
    def table_selector(self) -> Optional[str]:
        # strategy: return a table name -> name.col where col in name.
        
        table_exprs_in_scope = get_tables_from_scope(self.target_scope)
        tables_in_scope = get_all_table_names(table_exprs_in_scope)
        tables_in_scope = map(str.lower, tables_in_scope)
        
        final_tables:Set[str] = set()
        for table, col, distance in self.candidates:
            assert distance == 0
            if table.lower() in tables_in_scope:
                final_tables.add(table)
                
        if len(final_tables) == 1:
            return final_tables.pop()
        else:
            # exist multiple tables in the scope match the column
            # call llm
            return None
                
    
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        db:Database = DBs[db_id]
        assert self.table_identifier_type != Table_Identifier_Type.EMPTY
        # t, c = split_tableIdentifier_columnIdentifier(self.error_tc)
        
        # make sure table and column are mismatched!
        try:
            db.get_column_info(self.error_tc['table_name'], self.error_tc['column_name'])
            raise Exception
        except NoSuchColumnError as nsce:
            pass
        
        new_table = self.table_selector()
        if new_table is None:
            before = sql.statement
            self.fake_repair(sql, self.target_scope, self.error_tc)
            self.logging(sql.question_id, before, sql.statement, True, {})
            return sql, originalres
        
        try:
            new_table_identifier = mapping_name2alias(new_table, self.target_scope)
        except:
            new_table_identifier = new_table
        
        replace_flag = False
        for col in self.target_scope._raw_columns:
            if col.name.lower() == self.error_tc['column_name'].lower() and col.table.lower() == self.error_tc['table_identifier'].lower():
                if isinstance(col.parent, sqlglot.expressions.EQ) and isinstance(col.parent.parent, sqlglot.expressions.Join):
                    if col.parent.left.table == new_table_identifier or col.parent.right.table == new_table_identifier:
                        if self.otcmr.detect(sql, gold_sql, db_id, originalres):
                            self.otcmr.join_expr = col.parent.parent
                            self.otcmr.col_identifier = self.error_tc['column_name']
                            self.otcmr.table_identifier = self.error_tc['table_identifier']
                            self.otcmr.target_scope = self.target_scope
                            self.otcmr.generate_llm_prompt(sql)
                            before = sql.statement
                            self.fake_repair(sql, self.target_scope, self.error_tc)
                            self.logging(sql.question_id, before, sql.statement, True, {})
                            return sql, originalres
                        
                col.set('this', sqlglot.expressions.to_identifier(self.error_tc['column_name']))
                col.set('table', sqlglot.expressions.to_identifier(new_table_identifier))
                # col.replace(new_col)
                replace_flag = True
        if not replace_flag:
            raise Exception("replace doesn't happen.")
        
        before = sql.statement
        sql.partial_update()
        res, errmsg = DBs[db_id].execution_match(sql.statement, gold_sql)
        
        self.logging(sql.question_id, before, sql.statement, False, {})
        self.repair_update(sql, gold_sql, db_id, originalres, res)
        return sql, res

class Alias_Not_Use_Repairer(RepairerBase):
    def __init__(self):
        super().__init__()
        self.table_identifier_type: Table_Identifier_Type = None
        self.target_scope: sqlglot.optimizer.scope.Scope = None
        self.error_tc = {'table_identifier': None, 'table_name':None, 'column_identifier': None, 'column_name': None}
        
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        detected = True
        self.detect_update(sql, gold_sql, db_id, detected, originalres)
        return detected
        
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        assert self.table_identifier_type == Table_Identifier_Type.NAME
        
        db:Database = DBs[db_id]
        
        # assert c in t, if not, NoSuchColumnError will be throw.
        db.get_column_info(self.error_tc['table_name'], self.error_tc['column_name'])
        
        # assert t has alias
        
        table_with_alias = None
        
        for alias_or_name, expr in self.target_scope.sources.items():
            if isinstance(expr, sqlglot.expressions.Table):
                if expr.name == self.error_tc['table_name']:
                    if expr.alias != '':
                        table_with_alias = expr
                    
        if table_with_alias is None:    # assert t has alias
            return sql, originalres
                
        entered = False
        
        for col in self.target_scope._raw_columns:
            if col.name == self.error_tc['column_name']:
                if col.table == self.error_tc['table_name']:
                    # new_col = sqlglot.expressions.column(col=self.error_tc['column_name'], table=table_with_alias.alias, copy=False)
                    # col.replace(new_col)
                    
                    col.set('this', sqlglot.expressions.to_identifier(self.error_tc['column_name']))
                    col.set('table', sqlglot.expressions.to_identifier(table_with_alias.alias))
                    
                    entered = True
        
        assert entered
        
        before = sql.statement
        sql.partial_update()
        res, errmsg = DBs[db_id].execution_match(sql.statement, gold_sql)
        
        self.logging(sql.question_id, before, sql.statement, False, {})
        self.repair_update(sql, gold_sql, db_id, originalres, res)
        return sql, res

class Column_Spell_Error_Repairer(RepairerBase):
    def __init__(self):
        super().__init__()
        # it's caller's job to set these attributes.
        self.error_type = None
        self.candidates = []
        self.table_identifier_type: Table_Identifier_Type = None
        self.target_scope:sqlglot.optimizer.scope.Scope = None
        self.error_tc = None    # "table.column" or "column"
        
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        # Detect by error_dispatcher()
        detected = True
        self.detect_update(sql, gold_sql, db_id, detected, originalres)
        return detected
    
    def selector(self) -> Optional[Tuple[str, str, int]]:
        # select a candidate
        if len(self.candidates) == 1:
            return self.candidates[0]
        return None
    
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        table_exprs_in_scope = get_tables_from_scope(self.target_scope)
        tables_in_scope = get_all_table_names(table_exprs_in_scope)
        
        candidate = self.selector()
        
        if candidate is None:
            # Call llm
            before = sql.statement
            self.fake_repair(sql, self.target_scope, self.error_tc)
            self.logging(sql.question_id, before, sql.statement, True, {})
            return sql, originalres
        
        table, column, dist = candidate
        if table in tables_in_scope:
            try:
                # FIXME: mapping_name2alias should proceed in scope!
                table_identifier = mapping_name2alias(table, self.target_scope)
            except NoAliasError as nae:
                table_identifier = table
        else:
            # after in this branch, Missing JOIN will be called after.
            table_identifier = table
            
        for col in self.target_scope._raw_columns:
            if col.name == self.error_tc['column_name']:
                col.set('this', sqlglot.expressions.to_identifier(self.candidates[0][1]))
                col.set('table', sqlglot.expressions.to_identifier(table_identifier))
        
        before = sql.statement
        sql.partial_update()
        res, errmsg = DBs[db_id].execution_match(sql.statement, gold_sql)
        
        self.logging(sql.question_id, before, sql.statement, False, {})
        self.repair_update(sql, gold_sql, db_id, originalres, res)
        return sql, res
    
class Identifier_Hallucination_Repairer(RepairerBase):
    def __init__(self):
        super().__init__()
        self.target_scope:sqlglot.optimizer.scope.Scope = None
        self.error_tc:dict = None
        
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        detected = True
        self.detect_update(sql, gold_sql, db_id, detected, originalres)
        return detected
    
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        col_to_be_repair = None
        # col_to_be_repair need not to be a list.
        # if multiple cols need to be repaired, it will enter this function again!
        for col in self.target_scope._raw_columns:
            if col.name.lower() == self.error_tc['column_name'].lower() and col.table.lower() == self.error_tc['table_identifier'].lower():
                col_to_be_repair = col
        assert col_to_be_repair and "col_to_be_repair is not None!!!"
                
        db:Database = DBs[db_id]
        new_table_identifier = None
        for alias_or_name, expr in self.target_scope.sources.items():
            if isinstance(expr, sqlglot.expressions.Table):
                if db.column_in_table(expr.name, self.error_tc['column_name']):
                    new_table_identifier = alias_or_name
            elif isinstance(expr, sqlglot.optimizer.scope.Scope):
                if alias_or_name == '':
                    ...
                else:
                    assert isinstance(expr.expression, sqlglot.expressions.Select)
                    for sub_expr in expr.expression.expressions:
                        if isinstance(sub_expr, sqlglot.expressions.Column):
                            if self.error_tc['column_name'].lower() == sub_expr.name.lower():
                                new_table_identifier = alias_or_name
                        elif isinstance(sub_expr, sqlglot.expressions.Alias):
                            if self.error_tc['column_name'].lower() == sub_expr.alias.lower():
                                new_table_identifier = alias_or_name
                        else:
                            ...
            else:
                ... # ???
        
        col_to_be_repair.set('this', sqlglot.expressions.to_identifier(self.error_tc['column_name']))
        col_to_be_repair.set('table', sqlglot.expressions.to_identifier(new_table_identifier))
        
        before = sql.statement
        sql.partial_update()
        res, errmsg = DBs[db_id].execution_match(sql.statement, gold_sql)
        
        self.logging(sql.question_id, before, sql.statement, False, {})
        self.repair_update(sql, gold_sql, db_id, originalres, res)
        return sql, res
    
class Hallucination_Repairer(RepairerBase):
    def __init__(self):
        super().__init__()
        self.table_identifier_type: Table_Identifier_Type = None
        self.error_tc = {'table_identifier': None, 'table_name':None, 'column_identifier': None, 'column_name': None}
        self.target_scope:sqlglot.optimizer.scope.Scope = None
        
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        detected = True
        self.detect_update(sql, gold_sql, db_id, detected, originalres)
        return detected
    
    def selector(self, sql:SQL, candidates:dict) -> tuple[str, str]:    # return a table name
        # strategy
        return candidates[0]['table_name'], candidates[0]['col_name']
    
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        similar_col = DBs[db_id].col_vec_query(self.error_tc['column_name'], top_k=3)
        table_name, col_name = self.selector(sql, similar_col)  # TODO: [LLM Support], can use similar_col as recommendation.
        table_identifier = table_name
        
        try:
            table_identifier = mapping_name2alias(table_name, sql)
        except NoAliasError as e:
            ...                 # table_name in sql but without alias.
        except Exception as e:
            ...                 # table_name not in sql.
                
        for col in self.target_scope._raw_columns:
            if col.name == self.error_tc['column_name']:
                col.set('this', sqlglot.expressions.to_identifier(col_name))
                col.set('table', sqlglot.expressions.to_identifier(table_identifier))
        
        before = sql.statement
        sql.partial_update()
        res, errmsg = DBs[db_id].execution_match(sql.statement, gold_sql)
        
        self.logging(sql.question_id, before, sql.statement, False, {})
        self.repair_update(sql, gold_sql, db_id, originalres, res)
        return sql, res
    
class ON_TableColumn_Mimatch_Repairer(RepairerBase):
    def __init__(self):
        super().__init__()
        
        self.llm_prompt = ON_TableColumn_Mismatch_Prompt()
        self.join_expr:sqlglot.expressions.Join = None
        self.col_identifier:str = None
        self.table_identifier:str = None
        
        # pass by caller
        self.target_scope = None
        
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        detected = True
        self.detect_update(sql, gold_sql, db_id, detected, originalres)
        return detected
        
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        raise Exception("should never call this func.")
    
    def generate_llm_prompt(self, sql:SQL) -> None:            
        self.llm_prompt.set_params(self.join_expr, self.col_identifier, self.table_identifier, self.target_scope)
        prompt = self.llm_prompt.get_prompt()
        sql.repair_prompt.add(prompt)
    