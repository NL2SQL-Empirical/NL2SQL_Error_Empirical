import sqlglot
import sqlglot.expressions
import sqlglot.expressions
import sqlglot.optimizer
from sqlglot.optimizer.qualify import qualify
import sqlglot.optimizer.scope
import sqlglot.optimizer.simplify
from MapleRepair.Database import Database, DBs
from MapleRepair.Customized_Exception import NoSuchTableError
from sqlite3 import OperationalError
from func_timeout import FunctionTimedOut
from typing import List, Union, Dict, Set, Tuple
from MapleRepair.config import debugging
from MapleRepair.utils.sqlite_dialect import SQLite_Dialects
    
class SQL():
    def __init__(self, sql:str, question_id:int=None, db_id:str=None, gold_sql:str=None, question=None, evidence=None):
        self.statement = sql
        self.repairer_log = []
        self.parsed = None
        self.scope_root = None
        
        ### TODO: refactoring
        self.question_id = question_id
        self.gold_sql = gold_sql
        self.db_id = db_id
        self.question = question
        self.evidence = evidence
        ###
        
        # If not self.executable, self.execution_result is err_msg
        self.execution_result = None
        
        # Cache partial fetch! Guarantee for number at least 5!
        self.partial_result = None
        
        self.qualified:bool = False # column ambiguity / table existence
        self.unaliased:bool = False # subquery alias
        self.executable:bool = False # syntax error
        self._update_parse()
        
        # debug flag
        self.debug:bool = False
        
        """
        [
            {
                error description
                general repair instruction
                error in this SQL query
                specific repair instruction
                attempt repair SQL query
            }
        ]
        """
        self.repair_prompt:Set[str] = set()
        
        self.fake_mapping:List[Tuple[sqlglot.expressions.Expression, sqlglot.expressions.Expression]] = []
        
    def update(self, statement:str, repairer:str, errmsg:str, e:Exception=None):
        """
            Update all SQL attributes according to the new statement. In most of the cases, SQL.parsed changes first and then call this function.
        """
        self.repairer_log.append({"before": self.statement, "after": statement, "repairer": repairer, "errmsg": errmsg, "exception": e})
        self.statement = statement
        self._update_parse()
        
    def partial_update(self):
        """
            Update all vars based on self.parsed!
            self.parsed will not be modified!!!
            
            Useful in fake_repair!!!
        """
        self.statement = self.parsed.sql(dialect=SQLite_Dialects)
        
        self.executable = False
        self.execution_result = None
            
        self.scope_root = self._build_scope_root()        

        try:
            self.is_executable()
            # self.execution_result = self.execute()
            self.executable = True
        except FunctionTimedOut as fe:
            self.execution_result = 'timeout'
            self.executable = False
        except Exception as e:
            self.execution_result = str(e)
            self.executable = False
        
        if self.executable:
            self._qualify()
            self._identifier_unalias()
        
    def execute(self, fetch: Union[str, int] = "all") -> List:
        if self.execution_result:
            # TODO: partial result support
            if self.executable:
                if fetch == "all":
                    return self.execution_result
                elif fetch == "one":
                    return [self.execution_result[:1]]
                elif isinstance(fetch, int):
                    assert fetch > 0
                    if len(self.execution_result) > fetch:
                        return self.execution_result[:fetch]
                    return self.execution_result
                else:
                    assert False, "Invalid value of 'fetch'"
            else:
                # completely mimic sqlite3.execute
                raise OperationalError(self.execution_result)
        db:Database = DBs[self.db_id]
        if fetch == "all":
            self.execution_result = db.execute_query(query=self.statement, fetch=fetch, idx=self.question_id)
            self.partial_result = self.execution_result
            return self.execution_result
        elif fetch == "one":
            fetch = 1
        if fetch < 5:
            self.partial_result = db.execute_query(query=self.statement, fetch=5, idx=self.question_id)
        else:
            self.partial_result = db.execute_query(query=self.statement, fetch=fetch, idx=self.question_id)
        if isinstance(fetch, int):
            return self.partial_result[:fetch]
        raise Exception
        
    def _update_parse(self):
        self.executable = False
        self.execution_result = None
        self.partial_result = None
        
        try:
            self.parsed = sqlglot.parse_one(self.statement, read='sqlite')
            
            # some literal will be accidentally parsed to column, we make fix here. 
            for col_exp in self.parsed.find_all(sqlglot.expressions.Column):
                if "'" in col_exp.name:
                    col_exp.replace(sqlglot.expressions.Literal.string(col_exp.name))
                    
            # format here! quotation_hack will be removed here!
            self.statement = self.parsed.sql(dialect=SQLite_Dialects)
            
        except Exception as e:
            self.parsed = None
            
        self.scope_root = self._build_scope_root()        

        try:
            self.is_executable()
            # self.execution_result = self.execute()
            self.executable = True
        except FunctionTimedOut as fe:
            self.execution_result = 'timeout'
            self.executable = False
        except Exception as e:
            self.execution_result = str(e)
            self.executable = False
        
        if self.executable:
            self._qualify()
            self._identifier_unalias()
            
    def _build_scope_root(self) -> sqlglot.optimizer.scope.Scope:
        if self.parsed is None:
            return None
        try:
            scope = sqlglot.optimizer.scope.build_scope(self.parsed)
            return scope
        except Exception as e:
            return None
            
    def print_repair_log(self):
        s = '''
  Repairer: {repairer}
--------------------------------------------------
  Before: \033[31m{before}\033[0m
--------------------------------------------------
  Gold:  \033[32m{gold}\033[0m  
--------------------------------------------------
  After:  \033[33m{after}\033[0m
'''
        if self.repairer_log != []:
            print("##################################################")
            print(f"Question ID: {self.question_id}")
            print(f"Question: {self.question}")
        for log in self.repairer_log:
            print(s.format(repairer=log['repairer'], gold=self.gold_sql, before=log['before'], after=log['after']))
    
    def is_executable(self):
        """
        If SQL query is not executable, raise Exception.
        Else return True
        """
        db:Database = DBs[self.db_id]
        db.is_executable(query=self.statement, idx=self.question_id)
    
    def _qualify(self) -> None:
        try:
            qualified_ast = qualify(
                expression=self.parsed,
                schema=DBs[self.db_id]._schema4sqlglot,
                infer_schema=False,
                dialect=SQLite_Dialects
            )
            self.parsed = qualified_ast
            self.qualified = True
        except Exception as e:
            self.qualified = False
        
    def _identifier_unalias(self) -> None:
        self.unaliased = False
        if not self.parsed:    return
        if not self.qualified: return
        
        db:Database = DBs[self.db_id]
        
        root = self.scope_root
        
        for scope in root.traverse():
            for identifier in scope.find_all(sqlglot.expressions.Identifier):
                if identifier.arg_key != 'table':
                    continue
                
                if 'table_name' in identifier.args:
                    continue
            
                identifier_unaliased = False
                
                # if identifier itself is already table_name
                try:
                    db.get_columns_from_table(identifier.this)
                    identifier.set("table_name", identifier.this)
                    identifier.set("reference_table_name", identifier.this)
                    identifier_unaliased = True
                    continue
                except NoSuchTableError as nste:
                    # do nothing and search all alias then.
                    pass
                        
                for alias_or_name, table_expr in scope.sources.items():
                    if alias_or_name.lower() == identifier.this.lower():
                        if isinstance(table_expr, sqlglot.expressions.Table):
                            identifier.set("table_name", table_expr.name)
                            identifier.set("reference_table_name", table_expr.name)
                            identifier_unaliased = True
                            break
                        elif isinstance(table_expr, sqlglot.optimizer.scope.Scope):
                            # TODO: extract real table from subquery
                            # add a external var `real_table_name` only used for Inconsistent Join
                            col_name = identifier.parent.args['this'].this
                            subquery_selects = table_expr.expression.expressions
                            for subquery_select in subquery_selects:
                                alias = None
                                if isinstance(subquery_select, sqlglot.expressions.Alias):
                                    alias = subquery_select.alias
                                    subquery_select = subquery_select.this
                                if isinstance(subquery_select, sqlglot.expressions.Column):
                                    # subquery must be unaliased before
                                    if subquery_select.args['this'].this.lower() == col_name.lower():
                                        identifier.set("reference_table_name", subquery_select.args['table'].args['table_name'])
                            if "table_name" not in identifier.args and "reference_table_name" not in identifier.args:
                                # if alias to a calculation, then it will not be unaliased.
                                # e.g. (A.a / B.b) as alias
                                identifier_unaliased = False
                                break
                        else:
                            ...
                    else:
                        ...
                        
                if identifier_unaliased == False:
                    self.unaliased = False
                    return
                
        self.unaliased = True
        
    def _unalias_check(self):
        if debugging:
            root = self.scope_root
            for scope in root.traverse():
                for identifier in scope.find_all(sqlglot.expressions.Identifier):
                    if identifier.arg_key != 'table':
                        continue
                    if 'table_name' not in identifier.args:
                        raise Exception

    def get_used_columns(self) -> Dict[str, List[str]]:
        if not self.unaliased: return {}
        used_columns = {}
        try:
            for col_expr in self.parsed.find_all(sqlglot.expressions.Column):
                table = col_expr.args['table'].args['table_name']
                column = col_expr.args['this'].this
                if table not in used_columns:
                    used_columns[table] = set()
                used_columns[table].add(column)
        except Exception as e:
            used_columns = {}
        for table in used_columns:
            used_columns[table] = list(used_columns[table])
        return used_columns
        
    def make_fake_replace(self, new:sqlglot.expressions.Expression, old:sqlglot.expressions.Expression) -> None:
        """
        As long as make_fake_replace called, LLM must be involvoed in the repairing process!!!
        
        This function now can only process simple table-column mismatch!!!
        """
        try:
            assert any(id(c) == id(old) for c in self.parsed.find_all(sqlglot.expressions.Column))
        except AssertionError as ae:
            ...
        # old.set('this', sqlglot.expressions.to_identifier(new.args['this'].this))
        # old.set('table', sqlglot.expressions.to_identifier(new.args['table'].this))
        
        new = sqlglot.expressions.column(new.args['this'].this, new.args['table'].this, copy=False)
        # save mapping of (new, old)
        self.fake_mapping.append((new, old, self.execution_result))
        # replace old with new
        old.replace(new)
    
    def restore_fake_replace(self) -> None:
        """
        This function now can only process simple table-column mismatch!!!
        """
        # restore fake replace with mapping (new, old)
        while self.fake_mapping:
            new, old, _ = self.fake_mapping.pop()
            try:
                assert any(id(c) == id(new) for c in self.parsed.find_all(sqlglot.expressions.Column))
            except AssertionError as ae:
                ...
                
            # new.set('this', sqlglot.expressions.to_identifier(old.args['this'].this))
            # new.set('table', sqlglot.expressions.to_identifier(old.args['table'].this))
            new.replace(old)