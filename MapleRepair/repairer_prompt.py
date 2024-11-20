import json
import sqlglot
from MapleRepair.utils.sqlite_dialect import SQLite_Dialects
from typing import List, Tuple

class Subquery_MINMAX_Prompt():
    prompt = "It seems that you use MAX/MIN within sub queries to find the highest or lowest values based on a certain condition. Please use ORDER BY + LIMIT 1 when you need to find the highest or lowest values based on a certain condition instead of using MAX/MIN within sub queries.\n"
    
    def __init__(self):
        pass
    
    def set_params(self, suspect):
        pass
    
    def get_prompt(self,) -> str:
        return self.prompt
    
class Redundant_JOIN_Prompt():
    error_desc = """
Redundant JOIN refers to situations in SQL queries where certain tables are not effectively utilized. Typically, the columns from these tables only appear in the join conditions and are not referenced in the SELECT, WHERE, or other clauses, indicating that these tables may be redundant.

Another scenario occurs when some columns from these tables are referenced in the SELECT, WHERE, or other conditions, but those columns can be replaced with similar columns from other tables. After this substitution, the original tables become redundant again. Joining such unnecessary tables not only degrades query performance but can also lead to record loss leading to incorrect SQL query.
"""
    
    prompt = """
Possible Redundant JOIN Scope in SQL query is as follow:
```sql
{scope}
```

We find that the table `{table}` in this scope may be redundant, as the columns from the table `{table}` only appear in the join conditions and are not referenced in the SELECT, WHERE, or other clauses. 
- Review the question and evidence carefully to determine whether keep this table `{table}`.
- Keep the table if and only if the question or the evidence explicitly mentions this table and the purpose of joining this table serves as a filtering condition. (e.g. for table people and disease, a join operation of people and disease may serve as a filtering condition that people who get disease).
- If you decide to remove this table `{table}`, pay attention to the SQL query you modified and ensure that the table identifier and column correspond.
- If the table identifier and columns do not correspond, you will be punishable by death.
"""
    
    prompt2 = """
Possible Redundant JOIN Scope in SQL query is as follow:
```sql
{scope}
```

We find that the table `{table}` in thsi scope may be redundant. The columns from the table `{table}` appear in the SELECT, WHERE, or other clauses can be replaced with similar columns from other tables. 

Please review the question and evidence again to determine whether to replace those columns with their semantically similar columns. And after replacing, `{table}` may become redundant and you should review the question and evidence again to determine whether to remove this table.

- Review the question and evidence carefully to determine whether to replace the columns from the table `{table}` with similar columns from other tables.
- If you decide to replace all columns from the table `{table}` with similar columns from other tables, after replacing, please review the question and evidence carefully to determine whether to remove this table `{table}`.
- Keep the table if and only if the question or the evidence explicitly mentions this table and the purpose of joining this table serves as a filtering condition (e.g. for table people and disease, a join operation of people and disease may serve as a filtering condition that people who get disease).
- If you decide to remove this table `{table}`, pay attention to the SQL query you modified and ensure that the table identifier and column correspond.
- If the table identifier and columns do not correspond, you will be punishable by death.

All replaceable columns in table `{table}` and their semantically similar columns are as follow:
```json
{duplicates_info}
```
"""
    
    def __init__(self):
        pass
    
    def set_params(self, suspect) -> None:
        self.suspect = suspect
    
    def get_prompt(self) -> str:
        prompt = ""
        prompt += self.error_desc
        for scope, redundant_tables, used_table_statistics in self.suspect:
            for redundant_table in redundant_tables:
                duplicates_info = {redundant_table: {}}
                for column, item in used_table_statistics[redundant_table].items():
                    assert used_table_statistics[redundant_table][column]['has_duplicate'] == True
                    if column not in duplicates_info[redundant_table]:
                        duplicates_info[redundant_table][column] = set()
                    for dup_table, dup_column in used_table_statistics[redundant_table][column]['duplicates']:
                        duplicates_info[redundant_table][column].add(f"{dup_table}.{dup_column}")
                    duplicates_info[redundant_table][column] = list(duplicates_info[redundant_table][column])
                
                if used_table_statistics[redundant_table]:
                    prompt += self.prompt2.format(table=redundant_table, scope=scope.expression.sql(dialect=SQLite_Dialects), duplicates_info=json.dumps(duplicates_info))
                else:
                    prompt += self.prompt.format(table=redundant_table, scope=scope.expression.sql(dialect=SQLite_Dialects))
                
        return prompt

class Output_Format_Hallucination_Prompt():
    prompt = "This SQL query use `|| ' ' ||` to concatenate columns in the SELECT clause which is banned and using that is punishable by death. You should never use `|| ' ' ||` to concatenate columns in the SELECT clause. Please rewrite the SELECT clause without concatenating columns with `|| ' ' ||`!\n"
    
    def __init__(self):
        pass
    
    def set_params(self, T1_name:str, C1:str, T2_name:str, C2:str, on_expr:sqlglot.expressions.Expression):
        pass
    
    def get_prompt(self) -> str:
        return self.prompt
    
class Value_Specification_Prompt():
    error_desc = "Value Specification Error is that the value you selected in the SQL query does not exist in the specified column. This discrepancy can be attributed to four primary reasons: 1. The value you chose may be semantically similar to the values in the column but not an exact match. For instance, the column might contain 'Apple' while you searched for 'apple' (case sensitivity). 2. The value you are looking for might actually reside in a different column. Ensure that you are querying the correct column that contains the expected value. 3. The value itself is completely wrong. 4. The question itself asks you to use this value, which is typically enclosed in quotation marks."
    general_repair_instr = "For reason 1, you should consider using the similar value in the column. For reason 2, you should consider using the column that contains the identical value. For reason 3, you should use correct value according to the question and evidence. For reason 4, you should check the question again and make sure that the question itself asks you to use this value even if it is not existed in database."
    error_symptom = "The `{conditions}` in your SQL query is Value Specification Error."
    specific_repair_instr = "Based on the values you choose, we recommend you the syntactically similar value in the column, the semantically similar value in the column and the columns that identical value appears in. \n{recommendations}\n\nThere recommendation may or may not be correct for this question. You should reformulate the condition using the recommendation or reformulate it completely based on the question and evidence."
    specific_repair_instr_without_recommendations = "There is no similar value in the column and no identical value appears in other columns. You should check whether you completely get the value wrong (reason 3) or the question itself asks you to use this value (reason 4)."
    attempt_repair_sql = "The following SQL queries are attempts to repair the error in your SQL query. These SQL queries may not be correct, but they are suggestions to help you understand how to fix the error. \n{attempts}"
    
    def __init__(self):
        pass
    
    def set_params(self, conditions:sqlglot.expressions.Expression, recommendations: str, attempts: list[str] = None):
        self.conditions = conditions
        self.recommendations = recommendations
        self.attempts = attempts
    
    def get_prompt(self, enable_attempt:bool=False) -> str:
        if enable_attempt:
            if self.recommendations:
                prompt = '\n\n'.join([self.error_desc, self.general_repair_instr, self.error_symptom.format(conditions=self.conditions), self.specific_repair_instr.format(recommendations=self.recommendations), self.attempt_repair_sql.format(attempts='\n'.join(self.attempts))])
            else:
                prompt = '\n\n'.join([self.error_desc, self.general_repair_instr, self.error_symptom.format(conditions=self.conditions), self.specific_repair_instr_without_recommendations, self.attempt_repair_sql.format(attempts='\n'.join(self.attempts))])
        else:
            if self.recommendations:
                prompt = '\n\n'.join([self.error_desc, self.general_repair_instr, self.error_symptom.format(conditions=self.conditions), self.specific_repair_instr.format(recommendations=self.recommendations)])
            else:
                prompt = '\n\n'.join([self.error_desc, self.general_repair_instr, self.error_symptom.format(conditions=self.conditions), self.specific_repair_instr_without_recommendations])       
        # print(prompt)
        return prompt
    
class Inconsistent_Join_Prompt():
    error_desc = "Inconsistent ON Condition Error is that the ON condition of a JOIN operation is inconsistent and JOIN with such Inconsistent ON condition will not return any reasonable result."
    general_repair_instr = "Under normal circumstances, the ON condition of a JOIN operation should have some relationship between the two tables and Foreign Key constraints are most common relationships. Please check the relationship between the two tables and make sure the ON condition has relationship and is correct. If there is no relationship between the two tables, you may need to join more tables to get the correct relationship between two tables."
    error_symptom = "In this SQL query, `{T1}.{C1}` is irrelavant to `{T2}.{C2}`. So the ON condition of a JOIN operation `{on_expr}` is inconsistent and may not be correct."
    specific_repair_instr = "You should choose the correct columns from tables {T1} and {T2} to form the correct ON condition. If there is no relationship between tables {T1} and {T2}, you may need to join more tables to get the correct relationship between  {T1} and {T2}."
    attempt_repair_sql = ""
    
    def __init__(self):
        pass
    
    def set_params(self, T1_name:str, C1:str, T2_name:str, C2:str, on_expr:sqlglot.expressions.Expression):
        self.T1_name = T1_name
        self.C1 = C1
        self.T2_name = T2_name
        self.C2 = C2
        self.on_expr = on_expr.sql(dialect=SQLite_Dialects)
        # self.fk_direct_relationships:list = fk_direct_relationships
    
    def get_prompt(self) -> str:
        dk_str = ""
        # for fk in self.fk_direct_relationships:
        #     from_table, fk_info = fk.items()
        #     from_col, to_table, to_col = fk_info['from_col'], fk_info['to_table'], fk_info['to_col']
        #     dk_str += f"{from_table}.{from_col} references {to_table}.{to_col}, {from_table}.{from_col} = {to_table}.{to_col}\n"
        # prompt = '\n'.join([self.error_desc, self.general_repair_instr, self.error_symptom, self.specific_repair_instr.format(T1=self.T1_name, C1=self.C1, T2=self.T2_name, C2=self.C2, fk_relationships=dk_str)])
        prompt = '\n\n'.join([self.error_desc, self.general_repair_instr, self.error_symptom.format(T1=self.T1_name, C1=self.C1, T2=self.T2_name, C2=self.C2, on_expr=self.on_expr), self.specific_repair_instr.format(T1=self.T1_name, C1=self.C1, T2=self.T2_name, C2=self.C2)])
        # print(prompt)
        return prompt
    
    
class Inconsistent_IN_Prompt():
    error_desc = "Inconsistent IN Error is that the two columns between IN operator are unrelated. Two unrelated columns between IN operator will cause the query to return an empty result."
    general_repair_instr = "Two columns between IN operator should be related. Please consider use related columns between IN operator."
    error_symptom = "In this SQL query, two columns `{T1_name}.{C1}` and `{T2_name}.{C2}` between `{in_expr}` are unrelated. We consider this is an Inconsistent IN error."
    specific_repair_instr = "Two columns `{T1_name}.{C1}` and `{T2_name}.{C2}` between `{in_expr}` are unrelated. Please consider use related columns between IN operator."
    attempt_repair_sql = ""
    
    def __init__(self):
        pass
    
    def set_params(self, T1:str, C1:str, T2:str, C2:str, in_expr:sqlglot.expressions.In) -> None:
        self.T1 = T1
        self.T2 = T2
        self.C1 = C1
        self.C2 = C2
        self.in_expr = in_expr
    
    def get_prompt(self) -> str:
        prompt = '\n\n'.join([
            self.error_desc,
            self.general_repair_instr,
            self.error_symptom.format(T1_name=self.T1, T2_name=self.T2, C1=self.C1, C2=self.C2, in_expr=self.in_expr.sql(dialect=SQLite_Dialects)),
            self.specific_repair_instr.format(T1_name=self.T1, T2_name=self.T2, C1=self.C1, C2=self.C2, in_expr=self.in_expr.sql(dialect=SQLite_Dialects))
        ])
        # print(prompt)
        return prompt
    
class Comparison_Misuse_Prompt():
    error_desc = "Comparison Misuse Error is that the column used in comparison (<=> operator, MIN/MAX) is not orderable. Numeric types (INT, REAL, ...) are orderable obviously, but some column in TEXT types are also orderable."
    general_repair_instr = "Orderable columns should be used in comparison operators. Please check the columns used in comparison operators and replace those are not orderable. Then rewrite the rest part of sql query if it is needed based on the question and evidence."
    error_symptom = "In this SQL query, `{table}.{col}` in `{compare_expr}` is not orderable. It is considered as a comparison misuse error."
    specific_repair_instr = "Please replace `{table}.{col}` in `{compare_expr}` with a orderable column and rewrite the rest part of sql query if it is needed based on the question and evidence."
    attempt_repair_sql = ""
    
    def __init__(self):
        pass
    
    def set_params(self, table:str, column:str, compare_expr:sqlglot.expressions.Expression) -> None:
        self.table = table
        self.column = column
        self.compare_expr = compare_expr.sql("sqlite")
    
    def get_prompt(self) -> str:
        prompt = '\n\n'.join([self.error_desc, self.general_repair_instr, self.error_symptom.format(table=self.table, col=self.column, compare_expr=self.compare_expr), self.specific_repair_instr.format(table=self.table, col=self.column, compare_expr=self.compare_expr)])
        return prompt
    

class AggFunc_Misuse_Prompt():
    error_desc = "Aggregate Function Misuse Error is that the column used in Aggregate Function are not valid. For example, the aggregate functions `AVG` or `SUM` are used on a column that contains non-numeric values."
    general_repair_instr = "Orderable columns should be used in comparison operators. Please check the columns used in comparison operators and replace those are not orderable."
    error_symptom = "In this SQL query, `{table}.{col}` in `{agg_expr}` contains non-numeric values. It is considered as a comparison misuse error."
    specific_repair_instr = "Please replace `{table}.{col}` in `{agg_expr}` with a proper column whose values are numeric based on the question and evidence."
    attempt_repair_sql = ""
    
    def __init__(self):
        pass
    
    def set_params(self, table:str, col:str, agg_expr:sqlglot.expressions.Expression) -> None:
        self.table = table
        self.column = col
        self.agg_expr = agg_expr
    
    def get_prompt(self) -> str:
        prompt = '\n\n'.join([
            self.error_desc,
            self.general_repair_instr,
            self.error_symptom.format(table=self.table, col=self.column, agg_expr=self.agg_expr),
            self.specific_repair_instr.format(table=self.table, col=self.column, agg_expr=self.agg_expr)
        ])
        # print(prompt)
        return prompt
    
class Bare_JOIN_Prompt():
    prompt = "This SQL query seems to join tables without on condition. This is a little bit wired. Please check the SQL query again to make sure it correctly answer the question!"
    
    def __init__(self):
        pass
    
    def set_params(self, suspect):
        pass
    
    def get_prompt(self,) -> str:
        return self.prompt
    
class Literal_JOIN_Prompt():
    prompt = "This SQL query seems to join tables with a wired on condition. This is a little bit wired since most of on conditions are equation of two columns. Please check the SQL query again to make sure it correctly answer the question!"
    
    def __init__(self):
        pass
    
    def set_params(self, suspect):
        pass
    
    def get_prompt(self,) -> str:
        return self.prompt
    
class ON_TableColumn_Mismatch_Prompt():
    prompt = """
The SQL query contains a specific syntax error due to the column `{c1}` does not exist in table `{t1}` for the `{t1}.{c1}` in the ON condition of the JOIN operation `{join_str}`. The reason you select column `{c1}` for table `{t1}` mistakenly is probably that you want to join table `{t1}` and `{t2}`. 

Please check whether you really want to join table `{t1}` and `{t2}` based on the question, evidence and database infomation. If so, the following two scenarios and their solutions may help you.

Incorrect Column Selection:
- Scenario: There is a column in `{t1}` and `{t2}` that can be used for joining (e.g., a foreign key), but the wrong column `{c1}` was chosen from `{t1}`.
- Solution: Identify the correct column in `{t1}` that can be used for joining with `{t2}` and update the ON condition accordingly.

No Direct Joining Column:
- Scenario: There is no direct joining column between `{t1}` and `{t2}`.
- Solution: Consider joining through an intermediary table that has relationships with both `{t1}` and `{t2}`. This may involve adding an additional JOIN clause to establish the connection.

It is also possible that you find `{t1}` or `{t2}` is not the correct table to answer the question. If so, the following scenario and its solution may help you.

Incorrect Table Selection:
- Scenario: Either `{t1}` or `{t2}` is not the correct table to answer the question.
- Solution: Review the query's requirements and select the appropriate tables. Rewrite the SQL query to ensure it uses the correct tables and columns.

Please resolve this syntax error and check the SQL query again to make sure it can answer the question correctly!
"""
    
    def __init__(self):
        self.target_scope = None
    
    def set_params(self, join_expr:sqlglot.expressions.Join, col, table, target_scope:sqlglot.optimizer.scope.Scope):
        self.target_scope = target_scope
        self.join_expr_str = join_expr.sql(dialect=SQLite_Dialects)
        
        self.c1:str = col
        self.t1:str = target_scope.sources[table].name
        
        on_expr = join_expr.args['on']
        left = on_expr.left
        right = on_expr.right
        
        left_table = left.table     # alias_or_name
        right_table = right.table   # alias_or_name
        
        left_table = target_scope.sources[left_table].name
        right_table = target_scope.sources[right_table].name
        
        if left_table.lower() != self.t1.lower():
            self.t2 = left_table
        else:
            self.t2 = right_table
    
    def get_prompt(self) -> str:
        prompt = self.prompt.format(
            t1 = self.t1,
            c1 = self.c1,
            t2 = self.t2,
            join_str = self.join_expr_str
        )
        # print(prompt)
        return prompt
    
class Empty_Result_Prompt():
    prompt = """
The return of SQL query should not be empty! You should check the SQL query again to make sure that this SQL query answer the question correctly!
"""
    
    def __init__(self):
        pass
    
    def set_params(self) -> None:
        raise NotImplementedError
    
    def get_prompt(self) -> str:
        return self.prompt
    
class Only_Single_NULL_Prompt():
    prompt = """
The return of SQL query should not be a single NULL value! You should check the SQL query again to make sure that this SQL query answer the question correctly!
"""
    
    def __init__(self):
        pass
    
    def set_params(self) -> None:
        raise NotImplementedError
    
    def get_prompt(self) -> str:
        return self.prompt
    
class Time_Function_Check_Prompt():
    literal_prompt = """
The argument of the time function {function} in {whole_expr} seems wrong. Time function {function} accepts time-values as either ISO-8601 text or as Julian day numbers. However, the argument is text {values}. Please check whether {values} is the correct augment. If so, you should not use {function} function. Or choose a correct augment for this question!
"""
    column_prompt = """
The argument of the time function {function} in {whole_expr} seems wrong. Time function {function} accepts time-values as either ISO-8601 text or as Julian day numbers. However, the argument {argument} has value like {values}. Please check whether {argument} is the correct augment. If so, you should not use {function} function. Or choose a correct augment for this question!
"""
    
    def __init__(self):
        self.suspect:List[Tuple[sqlglot.expressions.Expression, sqlglot.expressions.Expression, List[str]]] = []
    
    def set_params(self, suspect:List[Tuple[sqlglot.expressions.Expression, sqlglot.expressions.Expression, List[str]]]) -> None:
        self.suspect = suspect
    
    def get_prompt(self) -> str:
        for time_expr, param, row_example in self.suspect:
            function = time_expr.name
            whole_expr = time_expr.sql(dialect=SQLite_Dialects)
            aug = param.sql(SQLite_Dialects)
            value = row_example[0]
            if isinstance(param, sqlglot.expressions.Column):
                prompt = self.column_prompt.format(function=function, whole_expr=whole_expr, argument=aug, values=value)
            elif isinstance(param, sqlglot.expressions.Literal):
                prompt = self.literal_prompt.format(function=function, whole_expr=whole_expr, values=whole_expr)
            else:
                raise Exception
        return prompt
    
class Order_Select_Prompt():
    prompt = """
- Do not select extra columns that are not explicitly requested in the query.
- In sql, just select columns that are explicitly requested in the query.
"""
#     prompt = """
# - You should only include the column(s) used for sorting in the SELECT clause if the question specifically ask for them. Otherwise, omit these columns from the SELECT.
# - Just include the column name in the ORDER BY in the SELECT clause when explicitly asked in the question. Otherwise, do not include the column name in the SELECT clause.
# """
    
    def __init__(self):
        pass
    
    def set_params(self) -> None:
        pass
    
    def get_prompt(self) -> str:
        return self.prompt
        
class Complex_Division_Prompt():
    prompt = "This SQL query involves a complex division. You should carefully check both the numerator and denominator to ensure they align with the question and the evidence!"
    
    def __init__(self):
        pass
    
    def set_params(self, table:str, column:str, compare_expr:sqlglot.expressions.Expression) -> None:
        pass
    
    def get_prompt(self) -> str:
        return self.prompt