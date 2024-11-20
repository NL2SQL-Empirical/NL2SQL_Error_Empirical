import sqlglot.expressions
import sqlglot
from MapleRepair.repairer_base import RepairerBase
from MapleRepair.SQL import SQL
from MapleRepair.repairer_prompt import Subquery_MINMAX_Prompt

class Subquery_MINMAX_Repairer(RepairerBase):
    """
        target to Text-to-SQL error: ** Extreme Value Selection Ambiguity **
    """
    def __init__(self):
        super().__init__()
        self.llm_prompt = Subquery_MINMAX_Prompt()
        
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        if sql.parsed is None:
            return False
        if not sql.qualified:
            return False
        if not sql.unaliased:
            return False
        
        detect = False
        for subquery_exp in sql.parsed.find_all(sqlglot.expressions.Subquery):
            select_exp = subquery_exp.this
            if len(select_exp.expressions) != 1:
                break
            exp = select_exp.expressions[0]
            if isinstance(exp, sqlglot.expressions.Alias):
                exp = exp.this
            if isinstance(exp, (sqlglot.expressions.Max, sqlglot.expressions.Min)):
                detect = True
                break
            
        if sql.partial_result is not None:
            if len(sql.partial_result) <= 1:
                detect = False
        else:
            res = sql.execute(2)
            if len(res) <= 1:
                detect = False
        
        self.detect_update(sql, gold_sql, db_id, detect, originalres)
        return detect
        
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        prompt = self.llm_prompt.get_prompt()
        sql.repair_prompt.add(prompt)
        
        self.logging(sql.question_id, sql.statement, sql.statement, True, {})
        return sql, originalres