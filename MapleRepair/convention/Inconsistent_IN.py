import sqlglot
import sqlglot.expressions
from MapleRepair.repairer_base import RepairerBase
from MapleRepair.Database import Database, DBs
from MapleRepair.SQL import SQL
from typing import List, Set, Tuple
from MapleRepair.repairer_prompt import Inconsistent_IN_Prompt

sql_query_template = "SELECT 1 FROM `{T1}` JOIN `{T2}` ON `{T1}`.`{C1}` = `{T2}`.`{C2}` LIMIT 1"

class Inconsistent_IN_Repairer(RepairerBase):
    """
        target to Text-to-SQL error: **Inconsistent IN**
    """
    def __init__(self, mode='relax'):
        super().__init__()
        assert mode in ('relax', 'restrict')
        self.mode = mode
        self.suspect:List[sqlglot.expressions.In] = []
        self.llm_prompt = Inconsistent_IN_Prompt()
        self.inconsistent_join_pairs:tuple[tuple[str, str], tuple[str, str]] = [] # ((left_table, left_col), (right_table, right_col))
        
        self._cache:Set[Tuple[str, bool]] = set()
        
    def detect(self, sql:SQL, sql_gold:str, db_id:str, originalres:int) -> bool:
        if sql.parsed is None: return False
        if not sql.qualified:  return False
        if not sql.unaliased:  return False
        
        self.suspect:List[sqlglot.expressions.In] = []
        
        if sql.executable == False:
            return False
        
        db:Database = DBs[sql.db_id]
        
        root = sql.scope_root
    
        for scope in root.traverse():
            if not scope.is_subquery:
                continue
            if isinstance(scope.expression, (sqlglot.expressions.Intersect, sqlglot.expressions.Union, sqlglot.expressions.Except)):
                subquery = scope.expression.left
                assert isinstance(subquery, sqlglot.expressions.Select)
            else:
                assert isinstance(scope.expression, sqlglot.expressions.Select)
                if not isinstance(scope.expression.parent, sqlglot.expressions.Subquery):
                    continue
                assert isinstance(scope.expression.parent, sqlglot.expressions.Subquery)
                subquery = scope.expression
            
            in_exp = subquery.parent.parent
            if isinstance(in_exp, sqlglot.expressions.Subquery):
                in_exp = in_exp.parent
            if not isinstance(in_exp, sqlglot.expressions.In):
                continue
            
            assert len(subquery.expressions) == 1
            
            in_right_col_exp = subquery.expressions[0]
            AggFuncType = (sqlglot.expressions.Max, sqlglot.expressions.Min, sqlglot.expressions.Sum, sqlglot.expressions.Avg)
            if isinstance(in_right_col_exp, AggFuncType):
                in_right_col_exp = in_right_col_exp.args['this']
            if isinstance(in_right_col_exp, sqlglot.expressions.Alias):
                in_right_col_exp = in_right_col_exp.this
            assert isinstance(in_right_col_exp, sqlglot.expressions.Column)
            in_right_col_name = in_right_col_exp.args['this'].this
            in_right_col_table_name = in_right_col_exp.args['table'].args['table_name']
            
            in_left_col_exp = in_exp.args['this']
            in_left_col_name = in_left_col_exp.args['this'].this
            in_left_col_table_name = in_left_col_exp.args['table'].args['table_name']
            
            if in_left_col_table_name.lower() == in_right_col_table_name.lower() and in_left_col_name.lower() == in_right_col_name.lower():
                continue
            
            in_str = f"`{in_left_col_table_name}`.`{in_left_col_name}` = `{in_right_col_table_name}`.`{in_right_col_name}`".lower()
            
            if (in_str, True) in self._cache:
                self.suspect.append(in_str)
            elif (in_str, False) in self._cache:
                pass
            else:
                if self.mode == 'restrict':
                    # here we have in_left_col and in_right_col! check FK relationship.
                    if db.column_exist_fk_relationship(in_left_col_table_name, in_left_col_name, in_right_col_table_name, in_right_col_name):
                        self._cache.add((in_str, False))
                        continue
                    else:
                        self._cache.add((in_str, True))
                        self.suspect.append(in_exp)
                        # print(sql.statement)
                        
                elif self.mode == 'relax':
                    if db.column_exist_fk_relationship(in_left_col_table_name, in_left_col_name, in_right_col_table_name, in_right_col_name):
                        self._cache.add((in_str, False))
                        continue
                    
                    sql_query = sql_query_template.format(
                        T1=in_left_col_table_name,
                        C1=in_left_col_name,
                        T2=in_right_col_table_name,
                        C2=in_right_col_name
                    )
                    try:
                        res = db.execute_query(query=sql_query, idx=sql.question_id)
                        if res:
                            self._cache.add((in_str, False))
                        else:
                            self._cache.add((in_str, True))
                            self.suspect.append(in_exp)
                            # print(sql.statement)
                    except Exception as e:
                        # assert False, "Synthesis query should have no error!"
                        ...

        detected = len(self.suspect) > 0
        self.detect_update(sql, sql_gold, db_id, detected, originalres)
        return detected
    
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        for in_exp in self.suspect:
            if not isinstance(in_exp, sqlglot.expressions.In):
                continue
            
            assert len(in_exp.args['query'].this.expressions) == 1
            
            # in_right_col_exp = in_exp.args['query'].this.expressions
            in_right_col_exp = in_exp.args['query'].this.expressions[0]
            AggFuncType = (sqlglot.expressions.Max, sqlglot.expressions.Min, sqlglot.expressions.Sum, sqlglot.expressions.Avg)
            if isinstance(in_right_col_exp, AggFuncType):
                in_right_col_exp = in_right_col_exp.args['this']
            if isinstance(in_right_col_exp, sqlglot.expressions.Alias):
                in_right_col_exp = in_right_col_exp.this
            assert isinstance(in_right_col_exp, sqlglot.expressions.Column)
            in_right_col_name = in_right_col_exp.args['this'].this
            in_right_col_table_name = in_right_col_exp.args['table'].args['table_name']
            
            in_left_col_exp = in_exp.args['this']
            assert isinstance(in_left_col_exp, sqlglot.expressions.Column)
            in_left_col_name = in_left_col_exp.args['this'].this
            in_left_col_table_name = in_left_col_exp.args['table'].args['table_name']
            
            self.llm_prompt.set_params(in_left_col_table_name, in_left_col_name, in_right_col_table_name, in_right_col_name, in_exp)
            prompt = self.llm_prompt.get_prompt()
            sql.repair_prompt.add(prompt)
            
        self.logging(sql.question_id, sql.statement, sql.statement, True, {})
        return sql, originalres