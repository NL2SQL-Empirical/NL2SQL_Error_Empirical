from MapleRepair.SQL import SQL
from pathlib import Path
from typing import List, Tuple, Dict
from MapleRepair.Database import Database, DBs
from MapleRepair.const import base_prompt, sql_result_prompt, hint, cot_prompt
from MapleRepair.repairer_base import RepairerBase
import re
from MapleRepair.utils.llm_api import gpt_request
from MapleRepair.repairer_prompt import Empty_Result_Prompt, Only_Single_NULL_Prompt
from enum import Enum

class Suspicious_Type(Enum):
    Empty_Result = 1
    Only_Single_NULL = 2

class Suspicious_Repairer(RepairerBase):
    def __init__(self, debug:bool=False):
        super().__init__()
        self.debug:bool = debug
        self.empty_prompt = Empty_Result_Prompt()
        self.single_null_prompt = Only_Single_NULL_Prompt()
        self.suspicious_type:Suspicious_Type = None
    
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        if not sql.executable:
            self.suspicious_type = None
            return False
        if sql.repair_prompt:
            # This is not suspicious. SQL has repair_prompt is incorrect than suspicious.
            self.suspicious_type = None
            return False
        try:
            sql_result = sql.execute(2)
        except BaseException as be:
            return False
        if len(sql_result) == 0:
            # empty return
            self.suspicious_type = Suspicious_Type.Empty_Result
            self.detect_update(sql, gold_sql, db_id, True, originalres)
            return True
        elif len(sql_result) == 1:
            if sql_result[0][0] is None:
                # just return NULL
                self.suspicious_type = Suspicious_Type.Only_Single_NULL
                self.detect_update(sql, gold_sql, db_id, True, originalres)
                return True
        return False
    
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        if self.suspicious_type == Suspicious_Type.Empty_Result:
            prompt = self.empty_prompt.get_prompt()
            sql.repair_prompt.add(prompt) 
            details = {"type": "Empty Result"}
        elif self.suspicious_type == Suspicious_Type.Only_Single_NULL:
            prompt = self.single_null_prompt.get_prompt()
            sql.repair_prompt.add(prompt)
            details = {"type": "Only Single NULL Result"}
            
        self.logging(sql.question_id, sql.statement, sql.statement, True, details)
        return sql, originalres