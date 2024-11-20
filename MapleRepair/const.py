base_prompt = """
[Objective]
Your objective is to make sure a query follows the database admin instructions and use the correct conditions.
Please use valid SQLite syntax. All SQL must be in the markdown code block and indicate script type in the code block.
[Instruciton]
- When you need to find the highest or lowest values based on a certain condition, using ORDER BY + LIMIT 1 is prefered over using MAX/MIN within sub queries.
- Do not select extra columns that are not explicitly requested in the query.
- In sql, just select columns that are explicitly requested in the query.
- Make sure you only output the information that is asked in the question. If the question asks for a specific column, make sure to only include that column in the SELECT clause, nothing more.
- Predicted query should return all of the information asked in the question without any missing or extra information.
- No matter of how many things the question asks, you should only return one SQL query as the answer having all the information asked in the question, seperated by a comma.
- Using || ' ' ||  to concatenate is string is banned and using that is punishable by death. Never concatenate columns in the SELECT clause.
- If you are joining multiple tables, make sure to use alias names for the tables and use the alias names to reference the columns in the query. Use T1, T2, T3, ... as alias names.
- If you are doing a logical operation on a column, such as mathematical operations and sorting, make sure to filter null values within those columns.
- After all above instructions are done. You should read [Hint] carefully if [Hint] exists and follow the instructions in [Hint] to repair the query. Please be aware that [Hint] may not contain all errors in the query! You should check the query carefully and fix all errors.
[Query]
{query}
[Evidence]
{evidence}
[Database info]
{desc_str}
[Foreign keys]
{fk_str}
[old SQL]
```sql
{sql_statement}
```
"""

cot_prompt = """
Take a deep breath and think step by step to find the correct sqlite SQL query. If you follow all the instructions and generate the correct query, I will give you 1 million dollars.
"""

note_prompt = """
[Note]
{value_spec}
"""

sql_err_prompt = """
[Error message]
{err_msg}
"""

sql_result_prompt = """
[Query result]
{query_result}
"""

din_base_prompt = """
[Objective]
For the given question, use the provided tables, columns, foreign keys, and primary keys to fix the given SQLite SQL QUERY for any issues. If there are any problems, fix them. If there are no issues, return the SQLite SQL QUERY as is.
Please use valid SQLite syntax. All SQL must be in the markdown code block and indicate script type in the code block.
[Instruciton]
- Avoid redundant columns in SELECT clause, all of the columns should be mentioned in the question.
- Pay attention to the columns that are used for the JOIN by checking the Foreign keys.
- Pay attention to the columns that are used for the WHERE statement.
- Pay attention to the columns that are used for the GROUP BY statement.
- Pay attention to the columns that are used for the ORDER BY statement.
- check that all of the columns exist in the table and there are no typos.
- Use CAST when is needed.
- USE CASE WHEN is needed.
[Query]
{query}
[Evidence]
{evidence}
[Database info]
{desc_str}
[Foreign keys]
{fk_str}
[old SQL]
```sql
{sql}
```
"""

hint = """
[Hint]
{hint_msg}
"""

syntax_error_hint = """
[hint] This SQL query has syntax error(s).
Error message from SQLite: {err_msg}
Please check all syntax error(s) and other potential error(s) and then fix them.
"""

null_val_hint = """
[hint] There are null values in the result of this SQL query.
Please read the question again, add `IS NOT NULL` to remove NULL value if possible and check all possible error(s) then fix them.
"""

empty_result_hint = """
[hint] This SQL query return nothing. However this SQL query should return something to answer the question.
Please check all possible error(s) and then fix them.
"""