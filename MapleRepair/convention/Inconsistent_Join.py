import sqlglot
import sqlglot.expressions
from MapleRepair.repairer_base import RepairerBase
from MapleRepair.Database import Database, DBs
from MapleRepair.SQL import SQL
from MapleRepair.repairer_prompt import Inconsistent_Join_Prompt
from typing import Set, Tuple

sql_query_template = "SELECT 1 FROM `{T1}` JOIN `{T2}` ON `{T1}`.`{C1}` = `{T2}`.`{C2}` LIMIT 1"

class Inconsistent_Join_Repairer(RepairerBase):
    """
        target to Text-to-SQL error: **Inconsistent ON Condition**
    """
    def __init__(self, mode:str='relax'):
        super().__init__()
        self.mode = mode
        assert self.mode in ('restrict', 'relax')
        self.suspect = []
        self.llm_prompt = Inconsistent_Join_Prompt()
        
        self._cache:Set[Tuple[str, bool]] = set()
        
    def detect(self, sql:SQL, sql_gold:str, db_id:str, originalres:int) -> bool:
        if sql.parsed is None:
            return False
        if not sql.qualified:
            return False
        if not sql.unaliased:
            return False
        
        self.suspect = []
        db:Database = DBs[sql.db_id]
        
        for join_expr in sql.parsed.find_all(sqlglot.expressions.Join):
            if not 'on' in join_expr.args:
                # JOIN with ON is likely a error
                continue
            
            on_expr = join_expr.args['on']
            
            if not isinstance(on_expr, sqlglot.expressions.EQ):
                if isinstance(on_expr, (sqlglot.expressions.Or, sqlglot.expressions.And)):
                    # ON cond1 AND/OR cond2 ...
                    continue
                else:
                    continue
                    
            assert isinstance(on_expr, sqlglot.expressions.EQ)
            left, right = on_expr.left, on_expr.right
            if not isinstance(left, sqlglot.expressions.Column):
                continue
            if not isinstance(right, sqlglot.expressions.Column):
                continue
            
            assert isinstance(left, sqlglot.expressions.Column) and isinstance(right, sqlglot.expressions.Column)
            
            # unalias case should not happen here
            # calculation should never be on condition
            left_table, left_column = left.args['table'].args['reference_table_name'], left.args['this'].this
            right_table, right_column = right.args['table'].args['reference_table_name'], right.args['this'].this
            
            on_str = f"`{left_table}`.`{left_column}` = `{right_table}`.`{right_column}`".lower()
            if (on_str, True) in self._cache:
                self.suspect.append(on_expr)
            elif (on_str, False) in self._cache:
                pass
            else:            
                # Restrict: check fk
                if self.mode == 'restrict':
                    if not db.column_exist_fk_relationship(left_table, left_column, right_table, right_column):
                        # inconsistent on condition here
                        ...
                                        
                # Relax: check whether result is empty
                elif self.mode == 'relax':
                    if db.column_exist_fk_relationship(left_table, left_column, right_table, right_column):
                        self._cache.add((on_str, False))
                        continue
                    sql_query = sql_query_template.format(T1=left_table, C1=left_column, T2=right_table, C2=right_column)
                    try:
                        res = db.execute_query(query=sql_query, idx=sql.question_id)
                        if res:
                            self._cache.add((on_str, False))
                        else:
                            self._cache.add((on_str, True))
                            self.suspect.append(on_expr)
                    except Exception as e:
                        # assert False, "Synthesis query should have no error!"
                        ...
                else:
                    assert False and "mode in ('restrict', 'relax')"
        
        detect = len(self.suspect) != 0
        self.detect_update(sql, sql_gold, db_id, detect, originalres)
        return detect
        
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        db:Database = DBs[sql.db_id]
        for on_expr in self.suspect:
            left, right = on_expr.left, on_expr.right            
            assert isinstance(left, sqlglot.expressions.Column) and isinstance(right, sqlglot.expressions.Column)
            left_table, left_column = left.args['table'].args['reference_table_name'], left.args['this'].this
            right_table, right_column = right.args['table'].args['reference_table_name'], right.args['this'].this
            # fk_relationships = db.get_fk_relationship(left_table, right_table)
            self.llm_prompt.set_params(left_table, left_column, right_table, right_column, on_expr)
            prompt = self.llm_prompt.get_prompt()
            sql.repair_prompt.add(prompt)
            
        self.logging(sql.question_id, sql.statement, sql.statement, True, {})
        return sql, originalres