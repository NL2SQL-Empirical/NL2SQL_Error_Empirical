from MapleRepair.SQL import SQL
from pathlib import Path
from typing import List, Tuple, Dict
from MapleRepair.Database import Database, DBs
from MapleRepair.const import base_prompt, sql_result_prompt, hint, cot_prompt
from MapleRepair.repairer_base import RepairerBase
import re
from MapleRepair.utils.llm_api import gpt_request
from MapleRepair.config import result_root_dir
import os
import time
import sqlglot
from MapleRepair.utils.persistence import make_log
import json
from MapleRepair.utils.format import read_json
from MapleRepair.utils.sqlite_dialect import SQLite_Dialects

def parse_sql_from_string(input_string):
    sql_patterns = [r'```sql(.*?)```', r'```(.*?)```']
    all_sqls = []
    for sql_pattern in sql_patterns:
        for match in re.finditer(sql_pattern, input_string, re.DOTALL):
            all_sqls.append(match.group(1).strip())
        
        if all_sqls:
            return all_sqls[-1]
        else:
            ...
    return "error: No SQL found in the input string"

class LLM_Repairer(RepairerBase):
    def __init__(self, enable:bool=False):
        super().__init__()
        self.enable:bool = enable
        self.llm_failure = 0
        self.llm_call = 0
    
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        if sql.repair_prompt:
            return True
        return False
    
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        raise NotImplementedError

    def repair_with_gpt(self, sql: SQL, enable_log=True) -> Tuple[SQL, Dict]:  
        os.makedirs(result_root_dir / 'llm_logs', exist_ok=True)
        log_path = result_root_dir / "llm_logs" / f"{sql.question_id}.txt"
        
        before = sql.statement
        self.llm_call += 1
        db:Database = DBs[sql.db_id]
        prompt = base_prompt.format(
            query = sql.question,
            evidence = sql.evidence,
            desc_str = db.schema_prompt,
            fk_str = db.fk_prompt,
            sql_statement = sql.statement
        )
            
        try:
            query_result = sql.execute(10)
        except BaseException as be:
            query_result = str(be) 
        
        prompt += sql_result_prompt.format(query_result=query_result)
        if len(sql.repair_prompt) >= 2:
            ...
            
        final_prompt = prompt
        if sql.repair_prompt:
            error_prompt = ('\n\n' + '='*30 + '\n\n').join(sql.repair_prompt)
            final_prompt += hint.format(hint_msg=error_prompt)
        final_prompt += cot_prompt
        
        if self.enable:
            print(final_prompt)
            return sql, {}    # debugging
        
        start = time.perf_counter()
        llm_response, usage = gpt_request(final_prompt, log_path=log_path)
        end = time.perf_counter()
        overhead = end - start
        
        gpt_repaired_sql = parse_sql_from_string(llm_response)
        
        if 'error' in gpt_repaired_sql:
            self.llm_failure += 1
        
        sql.repair_prompt.clear()    # clear repair_prompt after call llm
        
        try:
            parsed_repaired_sql = sqlglot.parse_one(gpt_repaired_sql, read='sqlite')
            gpt_repaired_sql = parsed_repaired_sql.sql(dialect=SQLite_Dialects, comments=False)
        except BaseException as be:
            pass
        
        sql.update(gpt_repaired_sql, "syntax_GPT", None)
        
        llm_log = []
        log_path = result_root_dir / "llm_overhead" / f"{sql.question_id}.json"
        if log_path.exists():
            llm_log = read_json(log_path)
        llm_log.append(
            {
                "llm_usage": usage,
                "time": overhead
            }
        )
        content = json.dumps(llm_log)
        make_log(log_path, content)
        
        self.logging(sql.question_id, before, sql.statement, True, {})
        return sql, usage