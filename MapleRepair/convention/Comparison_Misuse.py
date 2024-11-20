import sqlglot.expressions
import sqlglot
from MapleRepair.repairer_base import RepairerBase
from MapleRepair.SQL import SQL
from MapleRepair.Database import Database, DBs
from MapleRepair.repairer_prompt import Comparison_Misuse_Prompt

ComparisonAggFuncSet = (
    sqlglot.expressions.Max,
    sqlglot.expressions.Min,
)

ComparisonOpsSet = (
    sqlglot.expressions.LT,
    sqlglot.expressions.LTE,
    sqlglot.expressions.GT,
    sqlglot.expressions.GTE
)
class Comparison_Repairer(RepairerBase):
    """
        target to Text-to-SQL error: **Comparison Misuse**
    """
    def __init__(self):
        super().__init__()
        self.suspect = []
        self.llm_prompt = Comparison_Misuse_Prompt()
        
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        if sql.parsed is None:
            return False
        if not sql.qualified:
            return False
        if not sql.unaliased:
            return False
        
        self.suspect = []
        db:Database = DBs[sql.db_id]
        
        for comparsion_op in ComparisonOpsSet:
            for compare_expr in sql.parsed.find_all(comparsion_op):
                left, right = compare_expr.left, compare_expr.right
                if not isinstance(left, sqlglot.expressions.Column):
                    continue
                if not isinstance(right, sqlglot.expressions.Literal):
                    continue
                assert isinstance(left, sqlglot.expressions.Column)
                assert isinstance(right, sqlglot.expressions.Literal)
                column_name = left.args['this'].this
                table_name = left.args['table'].args['table_name']
                if not db.is_orderable(table_name, column_name):
                    self.suspect.append(compare_expr)
                        
        for comparsion_func in ComparisonAggFuncSet:
            for func_expr in sql.parsed.find_all(comparsion_func):
                if func_expr.args['this'].key == 'column':
                    column_name = func_expr.args['this'].args['this'].this
                    table_name = func_expr.args['this'].args['table'].args['table_name']
                    if not db.is_orderable(table_name, column_name):
                        self.suspect.append(func_expr)
                        
        detect:bool = len(self.suspect) != 0
            
        self.detect_update(sql, gold_sql, db_id, detect, originalres)
        return detect
        
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        for compare_expr in self.suspect:
            if isinstance(compare_expr, ComparisonOpsSet):
                left, right = compare_expr.left, compare_expr.right
                assert isinstance(left, sqlglot.expressions.Column)
                assert isinstance(right, sqlglot.expressions.Literal)
                column_name = left.args['this'].this
                table_name = left.args['table'].args['table_name']
            elif isinstance(compare_expr, ComparisonAggFuncSet):
                assert compare_expr.args['this'].key == 'column'
                column_name = compare_expr.args['this'].args['this'].this
                table_name = compare_expr.args['this'].args['table'].args['table_name']
            else:
                ...
            self.llm_prompt.set_params(table_name, column_name, compare_expr)
            prompt = self.llm_prompt.get_prompt()
            # print(prompt)
            sql.repair_prompt.add(prompt)      
        
        self.logging(sql.question_id, sql.statement, sql.statement, True, {})
        return sql, originalres