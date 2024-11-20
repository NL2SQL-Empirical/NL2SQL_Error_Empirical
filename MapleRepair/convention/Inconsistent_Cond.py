import sqlglot
import sqlglot.expressions
from MapleRepair.SQL import SQL
from MapleRepair.Database import Database, DBs
from MapleRepair.repairer_base import RepairerBase
from func_timeout import FunctionTimedOut
import difflib
from typing import List, Tuple, Dict
from MapleRepair.repairer_prompt import Value_Specification_Prompt

COMPARE_OPS = (
    sqlglot.expressions.EQ,
    # sqlglot.expressions.NEQ,
    sqlglot.expressions.GT,
    sqlglot.expressions.GTE,
    sqlglot.expressions.LT,
    sqlglot.expressions.LTE,
    # sqlglot.expressions.In,
    sqlglot.expressions.Like
)

def check_valid_condition(sql:SQL, expr:sqlglot.expressions) -> bool:
    assert isinstance(expr, COMPARE_OPS)
    assert sql.qualified
    _expr = expr.copy()
    left, right = _expr.left, _expr.right
    assert isinstance(left, sqlglot.expressions.Column) and isinstance(right, sqlglot.expressions.Literal)
    
    left.args['table'].replace(
        sqlglot.expressions.to_identifier(
            name=left.args['table'].args['table_name'],
            quoted=left.args['table'].args['quoted']
        )
    )
    table = left.args['table'].this
                    
    query = f"SELECT 1 FROM `{table}` WHERE {_expr} LIMIT 1;"
    db:Database = DBs[sql.db_id]
    result = db.execute_query(query=query, idx=sql.question_id)
    if result:
        return True
    return False

def _find_most_syntactically_similar_value(target_value: str, candidate_values: List[str]) -> List[Dict[str, float]]:
    """
    Finds the most syntactically similar value to the target value from the candidate values.

    Args:
        target_value (str): The target value to match.
        candidate_values (List[str]): The list of candidate values.
    """
    result = []
    for value in candidate_values:
        result.append({
                'value': value,
                'similarity': difflib.SequenceMatcher(None, value.lower(), target_value.lower()).ratio() if value else 0
            }
        )
    result = sorted(result, key=lambda x:x['similarity'], reverse=True)
    return result

class Inconsistent_Condition_Repairer(RepairerBase):
    def __init__(self, enable_vector_search:bool=True):
        super().__init__()
        self.enable_vector_search = enable_vector_search
        self.suspect:List[sqlglot.expressions.Expression] = []
        self.llm_prompt = Value_Specification_Prompt()
        
    def detect(self, sql:SQL, sql_gold:str, db_id:str, originalres:int) -> bool:
        if sql.parsed is None: return False
        if not sql.qualified: return False
        if not sql.unaliased: return False
        self.suspect = []
        db:Database = DBs[db_id]
        for expr in sql.parsed.find_all(COMPARE_OPS):
            left = expr.left
            right = expr.right
            if isinstance(left, sqlglot.expressions.Column) and \
               isinstance(right, sqlglot.expressions.Literal):
                   
                table = left.args['table'].args['table_name']
                column = left.args['this'].this
                column_info = db.get_column_info(table, column)
                
                if not right.args['is_string']:
                    continue
                
                # if date/time format, skip
                if 'date_format' in column_info or 'time_format' in column_info:
                    continue
                
                if column_info['distinct_val']:
                    value = right.this
                    if value in column_info['distinct_val']:
                        pass
                    else:
                        self.suspect.append(expr)
                else:
                    try:
                        valid = check_valid_condition(sql, expr)
                    except FunctionTimedOut as fto:
                        continue
                    except Exception as e:
                        # assert False, "Execution error should never happen here,"
                        continue
                    
                    if not valid:
                        self.suspect.append(expr)
            
        detect = len(self.suspect) > 0
        self.detect_update(sql, sql_gold, db_id, detect, originalres)
        return detect
    
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        assert sql.parsed and sql.qualified and sql.unaliased
        syntactically_similarity_threshold = 0.6
        db:Database = DBs[sql.db_id]
        recommendations = []
        for expr in self.suspect:
            left = expr.left
            right = expr.right
            assert isinstance(left, sqlglot.expressions.Column) and \
                   isinstance(right, sqlglot.expressions.Literal)
            value = right.this
            table = left.args['table'].args['table_name']
            column = left.args['this'].this
            
            # def contains_no_digits(s):
            #     return not any(char.isdigit() for char in s)
            # if contains_no_digits(value):
            #     continue
            
            # search for "syntactically similar" value in current columns
            # search for "semantically similar" value in current columns   
            syntactically_similar_vals_in_current_column = []
            semantically_similar_vals_in_current_column = []
            column_info = db.get_column_info(table, column)
            if column_info['distinct_val'] is not None:
                values_with_similarity = _find_most_syntactically_similar_value(value, list(column_info['distinct_val']))
                for value_similarity_info in values_with_similarity:
                    syntactically_similar_val, similarity = value_similarity_info['value'], value_similarity_info['similarity']
                    if similarity > syntactically_similarity_threshold:
                        syntactically_similar_vals_in_current_column.append(syntactically_similar_val)
                    if similarity == 1:
                        syntactically_similar_vals_in_current_column = [syntactically_similar_val]
                        break
                  
                temp = []
                if self.enable_vector_search:
                    semantically_similar_vals_in_current_column = db.val_vec_query(table, column, value)
                    semantically_similar_vals_in_current_column = sorted(semantically_similar_vals_in_current_column, key=lambda x:x[1], reverse=True)
                    for semantically_similar_value, similarity in semantically_similar_vals_in_current_column:
                        if similarity >= 0.9:
                            temp.append(semantically_similar_value)
                        if similarity == 1:
                            temp = [semantically_similar_value]
                            break
                semantically_similar_vals_in_current_column = temp
            
            # search for "identical" value in all columns (except PK)
            identical_vals_in_all_columns = []
            for _table_name in db.schema.keys():
                for _column_name, column_info in db.schema[_table_name].items():
                    distinct_val = column_info['distinct_val']
                    if distinct_val is None:
                        continue
                    for val in distinct_val:
                        if value.lower() == val.lower():
                            identical_vals_in_all_columns.append({"table":_table_name, "column":_column_name, "value":val})
                            
            # search for "identical" value in all Primary Keys
            identical_vals_in_pks = []
            sql_query_template = "SELECT 1 FROM `{table}` WHERE `{column}` = \"{value}\";"
            for _table, fks in db.pkfk['pk_dict'].items():
                for _column in fks:
                    try:
                        res = db.execute_query(
                            query=sql_query_template.format(table=_table, column=_column, value=value),
                            idx=sql.question_id
                        )
                    except BaseException as be:
                        print(sql_query_template.format(table=_table, column=_column, value=value))
                        raise be
                    if res:
                        identical_vals_in_pks.append({"table":_table, "column":_column, "value":value})
        
            recommendations = []        
            syntactically_similar_vals_in_current_column = syntactically_similar_vals_in_current_column[:3] if len(syntactically_similar_vals_in_current_column) > 3 else syntactically_similar_vals_in_current_column  
            for semantically_similar_val in semantically_similar_vals_in_current_column:
                recommendations.append(f"{table}.{column} does not contain the value '{value}'. But semantically similar value in {table}.{column} is '{semantically_similar_val}'.")
            for syntactically_similar_val in syntactically_similar_vals_in_current_column:
                recommendations.append(f"{table}.{column} does not contain the value '{value}'. But syntactically similar value in {table}.{column} is '{syntactically_similar_val}'.")
            for identical_val in (identical_vals_in_all_columns + identical_vals_in_pks):
                recommendations.append(f"{table}.{column} does not contain the value '{value}'. But '{identical_val['value']}' exists in {identical_val['table']}.{identical_val['column']}.")
            recommendations = '\n'.join(recommendations)
        
        self.llm_prompt.set_params(expr, recommendations)
        prompt = self.llm_prompt.get_prompt()
        # print(prompt)
        sql.repair_prompt.add(prompt)
        
        self.logging(sql.question_id, sql.statement, sql.statement, True, {})
        return sql, originalres
    
class LLM_Selection_Prompt:
    pass