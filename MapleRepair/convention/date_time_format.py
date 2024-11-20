import re
import sqlglot
from typing import Optional, Callable, Iterator, Tuple
from datetime import datetime
from MapleRepair.repairer_base import RepairerBase
from MapleRepair.config import *
from MapleRepair.utils.format import *
from MapleRepair.Database import DBs
from MapleRepair.SQL import SQL
    
class Date_Time_Format_Repairer(RepairerBase):
    '''
        target to Text-to-SQL error: **Date Format Issue**
        
        1. Find the binary operator that use column in date type as its operand
            
        2. Check if the corresponding operand(literal) is in format that unaligned with column's format
            
        3. replace it with correct format
    '''
    def __init__(self):
        super().__init__()
        self.comparator = [
            sqlglot.exp.EQ,
            sqlglot.exp.NEQ,
            sqlglot.exp.GT,
            sqlglot.exp.GTE,
            sqlglot.exp.LT,
            sqlglot.exp.LTE,
            sqlglot.exp.In,
        ]
        self.detected = False


    def alter_date_time_format(self, literal: str, target_format: tuple[str, str]) -> str:
        '''
        Brief:
            Alter the date/time format to the correct format

        Args:
            literal (str): the original date/time literal
            target_format (str): the target date/time format

        Returns:
            str: the altered date/time literal
            
        '''
        regex_format, datetime_format = target_format
        for pattern, date_format in DATE_FORMATS + TIME_FORMATS:
            match = re.match(pattern, literal)
            if match:
                parsed_date = datetime.strptime(literal, date_format)
                formatted_literal = parsed_date.strftime(datetime_format)
                if date_format[-2:] == '%f':
                    formatted_literal = formatted_literal[:-3]
                if regex_format == r'(\d{1}):(\d{2}).(\d{3})':
                    formatted_literal = formatted_literal[1:]
                return formatted_literal

        return literal

    def transformer(self, node, db) -> sqlglot.Expression:
        """ check if the date literal is in the correct format
        correct it if it mismatch with the column's date format

        Args:
            node (sqlglot.exp): sub-expression
            db (str): database id

        Returns:
            sqlglot.Expression: the corrected expression
        """ 
        
        # binary operator
        if type(node) in self.comparator \
            and isinstance(node.expression, sqlglot.exp.Literal)\
            and isinstance(node.this, sqlglot.exp.Column):
            try:
                table:str = None
                column:str = None
                if node.this.table is not None:
                    table_name = node.this.args['table'].args['table_name']
                if node.this.this is not None:
                    column_name = node.this.name

                date_format = DBs[db].get_column_info(table_name, column_name).get('date_format', None)
                time_format = DBs[db].get_column_info(table_name, column_name).get('time_format', None)
                
                if date_format is not None:
                    corrected_literal = self.alter_date_time_format(node.expression.name, date_format)
                    literal = sqlglot.exp.Literal(this=corrected_literal, is_string=True)
                    new_node = type(node)(this=node.this, expression=literal)
                    
                    self.detected = self.detected or corrected_literal != node.expression.name
                    return new_node
                
                if time_format is not None:
                    corrected_literal = self.alter_date_time_format(node.expression.name, time_format)
                    literal = sqlglot.exp.Literal(this=corrected_literal, is_string=True)
                    new_node = type(node)(this=node.this, expression=literal)
                    
                    self.detected = self.detected or corrected_literal != node.expression.name
                    return new_node
            except Exception as e:
                pass
            
        # BETWEEN operator
        if isinstance(node, sqlglot.exp.Between)\
            and isinstance(node.this, sqlglot.exp.Column)\
            and isinstance(node.args['low'], sqlglot.exp.Literal)\
            and isinstance(node.args['high'], sqlglot.exp.Literal):
            try:
                table:str = None
                column:str = None
                if node.this.table is not None:
                    table_name = node.this.args['table'].args['table_name']
                if node.this.this is not None:
                    column_name = node.this.name

                
                date_format = DBs[db].get_column_info(table_name, column_name).get('date_format', None)
                time_format = DBs[db].get_column_info(table_name, column_name).get('time_format', None)
                
                if date_format is not None:
                    corrected_low = self.alter_date_time_format(node.args['low'].name, date_format)
                    corrected_high = self.alter_date_time_format(node.args['high'].name, date_format)
                    low_literal = sqlglot.exp.Literal(this=corrected_low, is_string=True)
                    high_literal = sqlglot.exp.Literal(this=corrected_high, is_string=True)
                    new_node = sqlglot.exp.Between(this=node.this, low=low_literal, high=high_literal)
                    
                    self.detected = self.detected or corrected_low != node.args['low'].name or corrected_high != node.args['high'].name
                    
                    return new_node
                
                if time_format is not None:
                    corrected_low = self.alter_date_time_format(node.args['low'].name, time_format)
                    corrected_high = self.alter_date_time_format(node.args['high'].name, time_format)
                    low_literal = sqlglot.exp.Literal(this=corrected_low , is_string=True)
                    high_literal = sqlglot.exp.Literal(this=corrected_literal, is_string=True)
                    new_node = sqlglot.exp.Between(this=node.this, low=low_literal, high=high_literal)
                    
                    self.detected = self.detected or corrected_low != node.args['low'].name or corrected_high != node.args['high'].name
                    
                    return new_node
            except Exception as e:
                pass
            
        return node


    def do_check(self, sql:sqlglot.Expression, db_id:str) -> tuple[str, bool]:
        repaired_expression = sql.transform(self.transformer, db_id)
        return str(repaired_expression), self.detected
        
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> Tuple[SQL, int]:
        if sql.parsed is None\
            or not sql.qualified\
            or not sql.unaliased:
            return sql, originalres
        
        before = sql.statement
        self.detected = False
        try:
            new_sql_statement, _ = self.do_check(sql.parsed, db_id)
        except Exception as e:
            self.exception_case.append((sql, db_id, e))
            print(f'Exception: {e} when repairing {sql.statement} with db_id: {db_id}')
            self.logging(sql.question_id, sql.statement, sql.statement, False, {"Exception": str(e)})
            return sql, originalres
        
        res, errmsg = DBs[db_id].execution_match(new_sql_statement, gold_sql)
        sql.update(new_sql_statement, "Date_Format_Repairer", errmsg)
        
        self.logging(sql.question_id, sql.statement, sql.statement, False, {})
        self.repair_update(sql, gold_sql, db_id, originalres, res)
        return sql, res
    
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        if sql.parsed is None\
            or not sql.qualified\
            or not sql.unaliased:
                return False
        
        self.detected = False
        parsed = sql.parsed.copy()
        try:
            _, has_changed = self.do_check(parsed, db_id)
        except Exception as e:
            self.exception_case.append((sql, db_id, e))
            print(f'Exception: {e} when detecting {sql.statement} with db_id: {db_id}')
            return False
        
        res = has_changed
        self.detect_update(sql, gold_sql, db_id, res, originalres)
        return res


    
    