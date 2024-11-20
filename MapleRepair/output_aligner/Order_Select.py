import sqlglot.expressions
import sqlglot
from MapleRepair.repairer_base import RepairerBase
from MapleRepair.SQL import SQL
from MapleRepair.repairer_prompt import Order_Select_Prompt

class Order_Select_Repairer(RepairerBase):
    """
        target to Text-to-SQL error: ** **
    """
    def __init__(self):
        super().__init__()
        self.llm_prompt = Order_Select_Prompt()
        
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        if sql.parsed is None:
            return False
        if not sql.qualified:
            return False
        if not sql.unaliased:
            return False
        
        detected = False
        
        parsed:sqlglot.expressions.Expression = sql.parsed
        
        if not isinstance(parsed, sqlglot.expressions.Select):
            detected = False
            self.detect_update(sql, gold_sql, db_id, detected, originalres)
            return detected
        
        select_list = parsed.args['expressions']
        if 'order' in parsed.args and parsed.args['order'] and len(select_list) > 1:
            # print(sql.statement)
            alias_maps = {}
            for select_exp in select_list:
                if isinstance(select_exp, sqlglot.expressions.Alias):
                    alias_maps[select_exp.args['alias'].this] = select_exp.args['this']
                elif isinstance(select_exp, sqlglot.expressions.Column):
                    ...
                else:
                    ...
            ordered_list = parsed.args['order'].expressions
            for ordered_exp in ordered_list:
                if ordered_exp.args['this'].this.this in alias_maps:
                    detected = True
            ...
            
        self.detect_update(sql, gold_sql, db_id, detected, originalres)
        return detected
        
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        prompt = self.llm_prompt.get_prompt()
        sql.repair_prompt.add(prompt) 
        
        self.logging(sql.question_id, sql.statement, sql.statement, True, {})
        return sql, originalres