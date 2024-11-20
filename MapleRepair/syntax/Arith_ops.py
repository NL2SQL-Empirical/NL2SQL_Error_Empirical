import re
from MapleRepair.repairer_base import RepairerBase
from MapleRepair.SQL import SQL
from MapleRepair.Database import DBs

operator_dict = {
    'DIVIDE': '/',
    'MULTIPLY': '*',
    'SUBTRACT': '-',
    'ADD': '+'
}

def arith_ops_repair(sql:str, db_id:str=None) -> str:
    new_sql = sql
    while arith_ops_detect(new_sql):
        match = re.search("(SUBTRACT|MULTIPLY|ADD|DIVIDE)", new_sql, re.IGNORECASE)
        match_end = match.end()
        stack = []
        op_start = match.start()
        ops_range = [-1, -1]    # sql[ops_range[0]:ops_range[1]] <==> [ops_range[0], ops_range[1])
        for i, c in enumerate(new_sql):
            if i < match_end:
                continue
            if c == '(':
                if len(stack) == 0:
                    ops_range[0] = i + 1
                stack.append((i, c))
            elif c == ')':
                stack.pop()
                if len(stack) == 0:
                    ops_range[1] = i
                    break
        stack = []
        target_comma_i = -1
        for i, c in enumerate(new_sql):
            if i < ops_range[0]:
                continue
            elif i >= ops_range[1]:
                break
            if c == '(':
                stack.append((i, c))
            elif c == ')':
                stack.pop()
            elif c == ',':
                if len(stack) == 0:
                    target_comma_i = i
                    break
        assert target_comma_i != -1
        left = new_sql[ops_range[0]:target_comma_i].strip() # [ops_range[0], target_comma_i)
        right = new_sql[target_comma_i + 1:ops_range[1]].strip()
        whole_op_expression = new_sql[op_start:ops_range[1]+1]
        
        new_sql = new_sql.replace(whole_op_expression, f"(({left}) {operator_dict[match.group().upper()]} ({right}))")
    assert not arith_ops_detect(new_sql)    # in case of recursive ops.
    return new_sql

pattern = r"(DIVIDE|MULTIPLY|SUBTRACT|ADD)\((.*),\s*(.*)\)"

def arith_ops_detect(sql:str, db_id:str=None) -> bool:
    return re.findall(pattern, sql, flags=re.IGNORECASE) != []

class Arith_Ops_Repairer(RepairerBase):
    """
        target to Text-to-SQL error: **Function Hallucination (Arith)**
    """
    def __init__(self):
        super().__init__()
        
    def detect(self, sql:SQL, sql_gold:str, db_id:str, originalres:int) -> bool:
        res = arith_ops_detect(sql.statement)
        self.detect_update(sql, sql_gold, db_id, res, originalres)
        return res
        
    def repair(self, sql:SQL, sql_gold:str, db_id:str, originalres:int) -> tuple[SQL, int]:
        before = sql.statement
        try:
            new_sql_statement = arith_ops_repair(sql.statement)
        except Exception as e:
            self.exception_case.append((sql.statement, db_id, e))
            self.logging(sql.question_id, before, sql.statement, False, {"Exception": str(e)})
            return sql, originalres
        res, errmsg = DBs[db_id].execution_match(new_sql_statement, sql_gold)
        sql.update(new_sql_statement, "Arith_Ops_Repairer", errmsg)
        
        self.logging(sql.question_id, before, sql.statement, False, {})
        self.repair_update(sql, sql_gold, db_id, originalres, res)
        return sql, res


