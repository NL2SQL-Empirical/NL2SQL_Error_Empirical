import sqlglot
from MapleRepair.utils.format import *
from MapleRepair.repairer_base import RepairerBase
from typing import Optional, Callable, Iterator
from MapleRepair.Database import Database, DBs
from MapleRepair.SQL import SQL
from functools import cache
from MapleRepair.utils.sqlite_dialect import SQLite_Dialects
from typing import Tuple

# 1. functools.warp: x
# 2. types.MethodType: x
# 3. monkey_patching: x

def postorder_dfs(
    self, prune: Optional[Callable[[sqlglot.Expression], bool]] = None
) -> Iterator[sqlglot.Expression]:
    """
    Args:
        prune (t.Optional[t.Callable[[Expression], bool]], optional): A function that takes a node as an argument and returns True if the node should be pruned from the traversal. Defaults to None.

    Returns:
        t.Iterator[Expression]: An iterator over the nodes of the tree in postorder.
    """
    stack = [self]
    while stack:
        node = stack.pop()
        if node != None:
            stack.append(node)
            stack.append(None)
            for v in node.iter_expressions(reverse=True):
                stack.append(v)
        else:
            node = stack.pop()
            yield node
            
def transform(self, fun: Callable, *args:any, copy: bool = True, **kwargs) -> sqlglot.Expression:
    """
    Visits all tree nodes (excluding already transformed ones)
    and applies the given transformation function to each node.

    Args:
        fun: a function which takes a node as an argument and returns a
            new transformed node or the same node without modifications. If the function
            returns None, then the corresponding node will be removed from the syntax tree.
        copy: if set to True a new tree instance is constructed, otherwise the tree is
            modified in place.

    Returns:
        The transformed tree.
    """
    root = None
    new_node = None

    for node in (self.copy() if copy else self).dfs(prune=lambda n: n is not new_node):
        parent, arg_key, index = node.parent, node.arg_key, node.index
        new_node = fun(node, *args, **kwargs)

        # if not root:
        #     root = new_node
        # elif new_node is not node:
        
        if parent:
            parent.set(arg_key, new_node, index)
        else:
            root = new_node
            
    assert root
    return root.assert_is(sqlglot.Expression)

def execute_subquery(query:str, db_id:str, idx) -> list:
    db:Database = DBs[db_id]
    try:
        result:list = db.execute_query(query, 2, idx)
    except Exception as e:
        # print(f'Execution Error: {e} when executing {query} in {db_id}')
        return []
    return result


def transformer(node, db, res, idx) -> sqlglot.Expression:
    if isinstance(node, sqlglot.exp.EQ) and isinstance(node.expression, sqlglot.exp.Subquery):
        result = execute_subquery(node.expression.this.sql(dialect=SQLite_Dialects), db, idx)
        # print(f'Execute: {node.expression.this} in {db} => {result}')
        if len(result) > 1:
            res[0] = True
            return sqlglot.exp.In(this=node.this, query=node.expression)
    return node

@cache
def run_patching(sql:SQL, db_id:str) -> tuple[bool, str]:
    res:list[bool] = [False]
    origin_dfs = sqlglot.Expression.dfs
    origin_transform = sqlglot.Expression.transform
    
    sqlglot.Expression.dfs = postorder_dfs
    sqlglot.Expression.transform = transform
    
    if sql.parsed is None:
        return (False, sql)
    
    expression = sql.parsed.copy()
    
    repaired_expression = expression.transform(transformer, db_id, res, sql.question_id)
    
    sqlglot.Expression.dfs = origin_dfs
    sqlglot.Expression.transform = origin_transform
    
    return (res[0], repaired_expression.sql(dialect=SQLite_Dialects))


def equal_misuse_detect(sql:SQL, db_id:str) -> bool:
    return run_patching(sql, db_id)[0]
    
def equal_misuse_repair(sql:SQL, db_id:str) -> str:
    return run_patching(sql, db_id)[1]


class Equal_Repairer(RepairerBase):
    """
        target to Text-to-SQL error: ** Using = instead of IN **
    """
    def __init__(self):
        super().__init__()
    
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        res = equal_misuse_detect(sql, db_id)
        self.detect_update(sql, gold_sql, db_id, res, originalres)
        return res
        
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> Tuple[SQL, int]:
        before = sql.statement
        try:
            new_sql_statement = equal_misuse_repair(sql, db_id)
        except Exception as e:
            self.exception_case.append((sql, db_id, e))
            self.logging(sql.question_id, before, sql.statement, False, {"Exception": str(e)})
            return sql, originalres
        res, errmsg = DBs[db_id].execution_match(new_sql_statement, gold_sql)
        sql.update(new_sql_statement, "Equal_Repairer", errmsg)
        
        self.logging(sql.question_id, before, sql.statement, False, {})
        self.repair_update(sql, gold_sql, db_id, originalres, res)
        sql._unalias_check()
        return sql, res