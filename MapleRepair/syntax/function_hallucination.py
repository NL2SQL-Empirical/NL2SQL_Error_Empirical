import re
from MapleRepair.repairer_base import RepairerBase
from MapleRepair.utils.format import *
from MapleRepair.Database import DBs
from MapleRepair.SQL import SQL
from enum import Enum
from sqlite3 import OperationalError

# 662: wrong table
# 898: CURRENT_TIMESTAMP
# 1199: equal scenario missed
# 1233:distinct missed & = was replaced with errorneous >=

class ErrorType(Enum):
    NO_SUCH_FUNCTION_DATE = 1
    NO_SUCH_FUNCTION_MONTH = 2
    NO_SUCH_FUNCTION_YEAR = 3

error_pattern:dict[re.Pattern] = {
    re.compile(r'no such function: date', re.I): ErrorType.NO_SUCH_FUNCTION_DATE,
    re.compile(r'no such function: month', re.I): ErrorType.NO_SUCH_FUNCTION_MONTH,
    re.compile(r'no such function: year', re.I): ErrorType.NO_SUCH_FUNCTION_YEAR
}

class Date_Function_Hallucination_Repairer(RepairerBase):
    """
        target to Text-to-SQL error: **Function Hallucination (Date)**
    """
    def __init__(self):
        super().__init__()
        self.error_type:ErrorType = None
        
        
    def replace_date(self, column:str) -> str:
        '''
        Brief:
            Replace the DATE function with STRFTIME('%Y-%m-%d', column)
        '''
        return f'STRFTIME(\'%d\', {column})'

    def replace_month(self, column:str) -> str:
        '''
        Brief:
            Replace the MONTH function with STRFTIME('%m', column)
        '''
        return f'STRFTIME(\'%m\', {column})'

    def replace_year(self, column:str) -> str:
        '''
        Brief:
            Replace the YEAR function with STRFTIME('%Y', column)
        '''
        return f'STRFTIME(\'%Y\', {column})'


    def replace_date_month_year(self, sql:str, error_type:ErrorType) -> str:
        '''
        Brief:
            Replace the DATE, MONTH, YEAR function with 
                    STRFTIME('%d', column)
                    STRFTIME('%m', column)
                    STRFTIME('%Y', column) based on error_type
        
        Args:
            sql (str): the original SQL statement
            error_type (ErrorType): the error type detected
            
        Returns:
            str: the new SQL statement with DATE/MONTH/YEAR replaced
        '''
        corrected_sql:str = None
        if error_type == ErrorType.NO_SUCH_FUNCTION_DATE:
            corrected_sql =  re.sub(r'DATE\((.*?)\)', lambda x: self.replace_date(x.group(1)), sql, flags=re.I)
        elif error_type == ErrorType.NO_SUCH_FUNCTION_MONTH:
            corrected_sql = re.sub(r'MONTH\((.*?)\)', lambda x: self.replace_month(x.group(1)), sql, flags=re.I)
        elif error_type == ErrorType.NO_SUCH_FUNCTION_YEAR:
            corrected_sql = re.sub(r'YEAR\((.*?)\)', lambda x: self.replace_year(x.group(1)), sql, flags=re.I)
        else:
            return sql
        
        # check if the date is compared with a number
        # if so, add quotes around the number to make it a string
        if re.search(r'STRFTIME\(.*\)\s?(>|<|>=|<=|=|!=)\s*(\d+)', corrected_sql, re.IGNORECASE):
            corrected_sql = re.sub(r'(STRFTIME\(.*\)\s?)(>|<|>=|<=|=|!=)\s*(\d+)', lambda x: f'{x.group(1)}{x.group(2)} \'{x.group(3)}\'', corrected_sql, re.IGNORECASE)
            
        return corrected_sql
    
    def repair(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> tuple[SQL, int]:
        before = sql.statement
        try:
            new_sql_statement = self.replace_date_month_year(sql.statement, self.error_type)
        except Exception as e:
            self.exception_case.append((sql, db_id, e))
            self.logging(sql.question_id, before, sql.statement, False, {"Exception": str(e)})
            return sql, originalres
        
        res, errmsg = DBs[db_id].execution_match(new_sql_statement, gold_sql)
        sql.update(new_sql_statement, "Date_Function_Hallucination_Repairer", errmsg)
        
        
        self.logging(sql.question_id, before, sql.statement, False, {})
        self.repair_update(sql, gold_sql, db_id, originalres, res)
        return sql, res
    
    def detect(self, sql: SQL, gold_sql: str, db_id: str, originalres: int) -> bool:
        try:
            sql.is_executable()
            error = None
        except OperationalError as oe:
            error = str(oe)
        
        res = False
        if error != None:
            for pattern, error_type in error_pattern.items():
                if pattern.match(error):
                    self.error_type = error_type
                    res = True
                    break
        self.detect_update(sql, gold_sql, db_id, res, originalres)
        return res