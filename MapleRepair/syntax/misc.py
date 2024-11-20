from MapleRepair.repairer_base import RepairerBase
from MapleRepair.SQL import SQL
from MapleRepair.Database import Database, DBs
from typing import Tuple
from sqlite3 import OperationalError
from func_timeout import FunctionTimedOut
from MapleRepair.const import base_prompt, sql_err_prompt, cot_prompt, hint

class Misc_Execution_Failure_Repairer(RepairerBase):
    """
    This repairer is designed for execution failure which can not be repaired by any other repairer.
    """
    def __init__(self):
        super().__init__()
        self.llm_prompt = Misc_Execution_Failure_Prompt()
        
    def detect(self, sql:SQL, sql_gold:str, db_id:str, originalres:int) -> bool:
        detect = True
        
        if sql.fake_mapping:
            err_msg = []
            for _, _, e in sql.fake_mapping:
                err_msg.append(e)
            err_msg = '\n'.join(err_msg)

            sql.restore_fake_replace()
            sql.partial_update()
            
            if err_msg:
                sql.execution_result = err_msg
            assert not sql.executable
        
        if sql.executable:
            detect = False
            
        self.detect_update(sql, sql_gold, db_id, detect, originalres)
        return detect
        
    def repair(self, sql:SQL, sql_gold:str, db_id:str, originalres:int) -> Tuple[SQL, str]:
        # placeholder to make sure assertion not fail
        sql.repair_prompt.add("\n")
        
        self.logging(sql.question_id, sql.statement, sql.statement, True, {})
        return sql, originalres

class Misc_Execution_Failure_Prompt():
    def __init__(self):
        pass
        
    def set_params(self, sql:SQL, db_id:str, err_msg:str):
        self.sql:SQL = sql
        self.db:Database = DBs[db_id]
        self.err_msg = err_msg
        
    def get_prompt(self) -> str:
        prompt = base_prompt.format(
            query = self.sql.question,
            evidence = self.sql.evidence,
            desc_str = self.db.schema_prompt,
            fk_str = self.db.fk_prompt,
            sql_statement = self.sql.statement
        )
        prompt += sql_err_prompt.format(err_msg=self.err_msg)
        prompt += cot_prompt
        print(prompt)
        return prompt