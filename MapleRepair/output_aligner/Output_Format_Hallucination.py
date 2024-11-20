import sqlglot.expressions
import sqlglot
from MapleRepair.repairer_base import RepairerBase
from MapleRepair.SQL import SQL
from MapleRepair.repairer_prompt import Output_Format_Hallucination_Prompt

class Output_Format_Hallucination_Repairer(RepairerBase):
    """
        target to Text-to-SQL error: ** Output Format Hallucination: || ' ' ||**
    """
    def __init__(self):
        super().__init__()
        self.llm_prompt = Output_Format_Hallucination_Prompt()
        
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        if sql.parsed is None:
            return False
        if not sql.qualified:
            return False
        if not sql.unaliased:
            return False
        
        detect = False
        if isinstance(sql.parsed, sqlglot.expressions.Select):
            for select_exp in sql.parsed.expressions:
                if isinstance(select_exp, sqlglot.expressions.Alias):
                    select_exp = select_exp.this
                if isinstance(select_exp, sqlglot.expressions.DPipe):
                    detect = True
                    break
        
        self.detect_update(sql, gold_sql, db_id, detect, originalres)
        return detect
        
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        prompt = self.llm_prompt.get_prompt()
        sql.repair_prompt.add(prompt) 
        
        self.logging(sql.question_id, sql.statement, sql.statement, True, {})
        return sql, originalres