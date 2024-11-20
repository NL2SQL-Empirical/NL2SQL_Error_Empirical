import json
import argparse
import sqlite3
import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import build_scope, find_all_in_scope
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.qualify_columns import Resolver
from sqlglot.optimizer.scope import Scope, build_scope
from sqlglot.errors import OptimizeError
from pprint import pprint
from pathlib import Path
import typing as t
from MapleRepair.config import openai_api_key, openai_base_url
from MapleRepair.utils.format import format_sql
from openai import OpenAI
from MapleRepair.SQL import SQL



def get_table_case_insensitive(self, column_name: str) -> t.Optional[exp.Identifier]:
    """
    Get the table for a column name.

    Args:
        column_name: The column name to find the table for.
    Returns:
        The table name if it can be found/inferred.
    """
    if self._unambiguous_columns is None:
        self._unambiguous_columns = self._get_unambiguous_columns(
            self._get_all_source_columns()
        )
    
    def find_case_insensitive(lst, target):
        target_lower = target.lower()
        for item in lst:
            if item.lower() == target_lower:
                return item
            
        return target
    
    column_name = find_case_insensitive(self._unambiguous_columns, column_name)

    table_name = self._unambiguous_columns.get(column_name)

    if not table_name and self._infer_schema:
        sources_without_schema = tuple(
            source
            for source, columns in self._get_all_source_columns().items()
            if not columns or "*" in columns
        )
        if len(sources_without_schema) == 1:
            table_name = sources_without_schema[0]

    if table_name not in self.scope.selected_sources:
        return exp.to_identifier(table_name)

    node, _ = self.scope.selected_sources.get(table_name)

    if isinstance(node, exp.Query):
        while node and node.alias != table_name:
            node = node.parent

    node_alias = node.args.get("alias")
    if node_alias:
        return exp.to_identifier(node_alias.this)

    return exp.to_identifier(table_name)

def _qualify_columns_case_insensitive(scope: Scope, resolver: Resolver) -> None:
    """Disambiguate columns, ensuring each column specifies a source"""
    for column in scope.columns:
        column_table = column.table
        column_name = column.name

        if column_table and column_table in scope.sources:
            source_columns = resolver.get_source_columns(column_table)
            if source_columns and column_name.lower() not in [source_column.lower() for source_column in source_columns] and "*" not in source_columns:
                raise OptimizeError(f"Unknown column: {column_name}")

        if not column_table:
            if scope.pivots and not column.find_ancestor(exp.Pivot):
                # If the column is under the Pivot expression, we need to qualify it
                # using the name of the pivoted source instead of the pivot's alias
                column.set("table", exp.to_identifier(scope.pivots[0].alias))
                continue

            column_table = resolver.get_table(column_name)

            # column_table can be a '' because bigquery unnest has no table alias
            if column_table:
                column.set("table", column_table)
        elif column_table not in scope.sources and (
            not scope.parent
            or column_table not in scope.parent.sources
            or not scope.is_correlated_subquery
        ):
            # structs are used like tables (e.g. "struct"."field"), so they need to be qualified
            # separately and represented as dot(dot(...(<table>.<column>, field1), field2, ...))

            root, *parts = column.parts

            if root.name in scope.sources:
                # struct is already qualified, but we still need to change the AST representation
                column_table = root
                root, *parts = parts
            else:
                column_table = resolver.get_table(root.name.lower())

            if column_table:
                column.replace(exp.Dot.build([exp.column(root, table=column_table), *parts]))

    for pivot in scope.pivots:
        for column in pivot.find_all(exp.Column):
            if not column.table and column.name in resolver.all_columns:
                column_table = resolver.get_table(column.name)
                if column_table:
                    column.set("table", column_table)
                    
################ rewrite the original functions (Case sensitive bug in sqlglot) ################
Resolver.get_table = get_table_case_insensitive
sqlglot.optimizer.qualify_columns._qualify_columns = _qualify_columns_case_insensitive
################ end of rewrite ################


        
class SQLiteTableColumnExtractor(DeprecationWarning):
    def __init__(self, db_path):
        """
        初始化 SQLite Table-Column 提取器。
        
        :param db_path: str, SQLite 数据库文件的路径。
        """
        self.db_path = db_path
        self.db_name = Path(db_path).stem
        self.connection = None
        self.schema = None
        
        self._connect()
        self.extract_schema()
        self._disconnect()

    def _connect(self):
        """
        连接到 SQLite 数据库。
        """
        self.connection = sqlite3.connect(self.db_path, isolation_level=None)

    def _disconnect(self):
        """
        断开与 SQLite 数据库的连接。
        """
        if self.connection:
            self.connection.close()

    def extract_schema(self)->dict[dict[str, str]]:
        """
        从 SQLite 数据库中提取 schema。
        返回一个字典，其中键是表名，值是一个字典，包含表的列名和数据类型。
        """
        self._connect()
        cursor = self.connection.cursor()

        schema = {}

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[0]
            cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
            columns = cursor.fetchall()
            schema[table_name] = {column[1]:column[2] for column in columns}

        self._disconnect()
        self.schema = schema
        return schema
    
    def qualifying(self, sql_query):
        try:
            ast = sqlglot.parse_one(sql_query, read="sqlite")
            qualified_ast = qualify(
                expression=ast,
                schema=self.schema,
                infer_schema=False
            )
        except Exception as e:
            print(f"\31[31mError: {e}\31[0m")
            qualified_ast = None
            
        return qualified_ast

    def extract_table_column_pairs(self, sql_query)->list[tuple[str, str]]:
        """
        从 SQL 查询中提取所有表-列对。
        
        :param sql_query: str, SQL 查询语句。
        :return: list, 以 (table, column) 形式的表-列对列表。
        """
        if self.schema is None:
            raise ValueError("Schema not loaded. Please run extract_schema() first.")

        qualified_ast = self.qualifying(sql_query)
        if qualified_ast is None:
            return None
        
        root = build_scope(qualified_ast)
        table_column_pairs = []
        
        for scope in root.traverse():
            for column in find_all_in_scope(scope.expression, exp.Column):
                current_scope = scope
                
                if column.table == "":
                    continue
                    
                while column.table not in current_scope.sources:
                    current_scope = current_scope.parent
                    if current_scope is None:
                        # can not find a corresponding table for the column in its ancestor scopes
                        raise ValueError(f"Table {column.table} not found in sources.")
                    
                # print(f"{column} => {current_scope.sources[column.table]}")
                current_scope = scope
                while column.table not in current_scope.sources:
                    current_scope = current_scope.parent
                    if current_scope is None:
                        # can not find a corresponding table for the column in its ancestor scopes
                        raise ValueError(f"Table {column.table} not found in sources.")
                
                if isinstance(current_scope.sources[column.table], exp.Table):
                    table_column_pairs.append((current_scope.sources[column.table].name, column.name))
        
        
        # get all the sources
        # for scope in root.traverse():
        #     for alias, (node, source) in scope.selected_sources.items():
        #         print(f'alias: {alias},\n node: {node},\n source: {source}\n type: {type(source)}')
        return table_column_pairs

    def get_column_pairs(self, identifier)->list[tuple[str, str]]:
        """
        从解析后的 Identifier 中提取字段对。
        
        :param identifier: sqlparse.sql.Identifier, SQL 标识符。
        :return: list, 以 (table, column) 形式的表-列对列表。
        """
        pairs = []
        name_parts = identifier.get_real_name().split('.')

        if len(name_parts) == 2:
            table_name, column_name = name_parts
            if table_name in self.schema and column_name in self.schema[table_name]:
                pairs.append((table_name, column_name))
        elif len(name_parts) == 1:
            column_name = name_parts[0]
            for table_name, columns in self.schema.items():
                if column_name in columns:
                    pairs.append((table_name, column_name))
        return pairs


def extract_table_column_pairs(sql_query:SQL)->list[tuple[str, str]]:
    """
    从 SQL 查询中提取所有表-列对。
    
    :param sql_query: str, SQL 查询语句。
    :return: list, 以 (table, column) 形式的表-列对列表。
    """
    qualified_ast = sql_query.parsed
    
    root = build_scope(qualified_ast)
    table_column_pairs = []
    
    for scope in root.traverse():
        for column in find_all_in_scope(scope.expression, exp.Column):
            current_scope = scope
            
            if column.table == "":
                continue
                
            while column.table not in current_scope.sources:
                current_scope = current_scope.parent
                if current_scope is None:
                    # can not find a corresponding table for the column in its ancestor scopes
                    raise ValueError(f"Table {column.table} not found in sources.")
                
            # print(f"{column} => {current_scope.sources[column.table]}")
            current_scope = scope
            while column.table not in current_scope.sources:
                current_scope = current_scope.parent
                if current_scope is None:
                    # can not find a corresponding table for the column in its ancestor scopes
                    raise ValueError(f"Table {column.table} not found in sources.")
            
            if isinstance(current_scope.sources[column.table], exp.Table):
                table_column_pairs.append((current_scope.sources[column.table].name, column.name))
    
    
    # get all the sources
    # for scope in root.traverse():
    #     for alias, (node, source) in scope.selected_sources.items():
    #         print(f'alias: {alias},\n node: {node},\n source: {source}\n type: {type(source)}')
    return table_column_pairs


def main():
    """
    main function: parse the arguments and run the extractor, then print the results(extracted list and qualified sql).

    :return: None
    """ 
    parser = argparse.ArgumentParser(description="Extract table-column pairs from SQL queries.")
    parser.add_argument("--db_path", type=str, help="Path to the SQLite database file.")
    parser.add_argument("--sql_query", type=str, help="SQL query to extract table-column pairs from.")
    args = parser.parse_args()
    

    extractor = SQLiteTableColumnExtractor(args.db_path)

    table_column_pairs = extractor.extract_table_column_pairs(args.sql_query)
    pprint(table_column_pairs)
    print(f'qualified_ast:\n{format_sql(extractor.qualifying(args.sql_query).sql())}')

if __name__ == "__main__": 
    main()
