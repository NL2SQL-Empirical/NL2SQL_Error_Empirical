from MapleRepair.Database import init_DBs
from MapleRepair.SQL import SQL
from typing import Tuple, List
from datetime import datetime
from pathlib import Path
from MapleRepair.utils.format import write_json
from pprint import pprint

### >>>>>>>>>>>>>>>>>  import repairers  >>>>>>>>>>>>>>>>>>>>>>>>>>
from MapleRepair.syntax.Arith_ops import Arith_Ops_Repairer
from MapleRepair.syntax.function_hallucination import Date_Function_Hallucination_Repairer
from MapleRepair.syntax.no_such_column import No_Such_Column_Repairer
from MapleRepair.syntax.misc import Misc_Execution_Failure_Repairer

from MapleRepair.logic.Cast import Div_Cast_Repairer
from MapleRepair.logic.check_equal import Equal_Repairer
from MapleRepair.logic.null_exclude import Null_Value_Repairer

from MapleRepair.convention.date_time_format import Date_Time_Format_Repairer
from MapleRepair.convention.Inconsistent_Join import Inconsistent_Join_Repairer
from MapleRepair.convention.Inconsistent_Cond import Inconsistent_Condition_Repairer
from MapleRepair.convention.Inconsistent_IN import Inconsistent_IN_Repairer
from MapleRepair.convention.Comparison_Misuse import Comparison_Repairer

from MapleRepair.semantic.suspicious import Suspicious_Repairer

from MapleRepair.output_aligner.Subquery_MINMAX import Subquery_MINMAX_Repairer
from MapleRepair.output_aligner.Output_Format_Hallucination import Output_Format_Hallucination_Repairer
from MapleRepair.output_aligner.Order_Select import Order_Select_Repairer

from MapleRepair.LLM import LLM_Repairer

### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    
class MapleRepair():
    def __init__(self, db_list:List[str]=None, result_root_dir:Path=None, LLM_enable:bool=True):
        # init_database
        init_DBs(db_list)
        
        self.result_root_dir = Path('result') / datetime.now().strftime("%Y-%m-%d-%H-%M-%S") if not result_root_dir else result_root_dir
        
        ### execution failure repairer
        self.nscr = No_Such_Column_Repairer()
        self.aor = Arith_Ops_Repairer()
        self.fhr = Date_Function_Hallucination_Repairer()
        self.mefr = Misc_Execution_Failure_Repairer()
        
        ### logic error repairer
        self.dcr = Div_Cast_Repairer()
        self.er = Equal_Repairer()
        self.nvr = Null_Value_Repairer()
        
        ### convenion error repairer  
        self.dtfr = Date_Time_Format_Repairer()   
        self.cr = Comparison_Repairer()
        self.iir = Inconsistent_IN_Repairer()
        self.ijr = Inconsistent_Join_Repairer()
        self.icr = Inconsistent_Condition_Repairer()
        
        ### semantic error repairer
        self.susr = Suspicious_Repairer()
        
        ### output aligner
        self.smr = Subquery_MINMAX_Repairer()
        self.ofhr = Output_Format_Hallucination_Repairer()
        self.osr = Order_Select_Repairer()
        
        ### LLM repairer
        self.llm = LLM_Repairer(enable=LLM_enable)
        
        self.syntax_repairers = []
        self.logic_repairers = []
        self.convention_repairers = []
        self.semantic_repairers = []
        self.output_aligners = []

        self.syntax_repairers = [self.aor, self.fhr, self.nscr, self.mefr]
        self.logic_repairers = [self.dcr, self.nvr, self.er]
        self.convention_repairers = [self.dtfr, self.icr, self.ijr, self.cr, self.iir]
            
        self.semantic_repairers = [self.susr]
        self.output_aligners = [self.smr, self.ofhr, self.osr]
            
        self.resolved_exception = {}
    
    def syntax_repair(self, sql: SQL, gold_sql:str, db_id:str, origin_res) -> Tuple[SQL, int]:
        # global exception_dict
        res = origin_res
        for repairer in self.syntax_repairers:
            # exception must be catched here
            # or sql query will not be repaired 
            # by other repairer!
            try:
                if repairer.detect(sql, gold_sql, db_id, res):
                    sql, res = repairer.repair(sql, gold_sql, db_id, res)
                    assert sql is not None
            except BaseException as be:
                if type(be).__name__ not in self.resolved_exception:
                    self.resolved_exception[type(be).__name__] = []
                self.resolved_exception[type(be).__name__].append((sql.question_id, str(be)))
        return sql, res
    
    def logic_repair(self, sql: SQL, gold_sql:str, db_id:str, origin_res) -> Tuple[SQL, int]:
        """
        Detect and repair logic errors. Repairing Logic errors does not need LLMs.
        """
        res = origin_res
        for repairer in self.logic_repairers:
            if repairer.detect(sql, gold_sql, db_id, res):
                sql, res = repairer.repair(sql, gold_sql, db_id, res)
            assert sql is not None
        return sql, res

    def convention_repair(self, sql: SQL, gold_sql:str, db_id:str, origin_res) -> Tuple[SQL, int]:
        """
        检测并修复约定错误。
        """
        res = origin_res
        for repairer in self.convention_repairers:
            if repairer.detect(sql, gold_sql, db_id, res):
                sql, res = repairer.repair(sql, gold_sql, db_id, res)
                assert sql is not None
        return sql, res
                
    def semantic_repair(self, sql: SQL, gold_sql:str, db_id:str, origin_res) -> Tuple[SQL, int]:
        """
        检测并修复约定错误。
        """
        res = origin_res
        for repairer in self.semantic_repairers:
            if repairer.detect(sql, gold_sql, db_id, res):
                sql, res = repairer.repair(sql, gold_sql, db_id, res)
                assert sql is not None
        return sql, res
                
    def output_align(self, sql:SQL, gold_sql:str, db_id:str, origin_res) -> Tuple[SQL, int]:
        res = origin_res
        for aligner in self.output_aligners:
            if aligner.detect(sql, gold_sql, db_id, res):
                sql, res = aligner.repair(sql, gold_sql, db_id, res)
                assert sql is not None
        return sql, res
    
    def repair(self, sql_statement: str, db_id:str, gold_sql:str=None, origin_res=None, question_id:int=None, question:str=None, evidence:str=None) -> str:
        """
        对外暴露的修复方法，执行检测-修复流程。
        """
        
        sql = SQL(sql=sql_statement, db_id=db_id, gold_sql=gold_sql, question=question, evidence=evidence, question_id=question_id)
        
        # repaired_sql = sql
        repaired_sql, res = self.syntax_repair(sql, gold_sql, db_id, origin_res)
            
        assert repaired_sql is not None
        # assert repaired_sql.executable
        
        if not repaired_sql.executable:
            assert repaired_sql.repair_prompt, "Unprocessed Excution Failure without repair prompt!"
            
        if self.llm.detect(repaired_sql, gold_sql, db_id, res):
            repaired_sql, usage = self.llm.repair_with_gpt(repaired_sql)
            
        if repaired_sql.executable == False:
            # for those can't repaired, return it without modification.
            return sql_statement
        
        # repaired_sql = sql
        repaired_sql, res = self.logic_repair(repaired_sql, gold_sql, db_id, res)
        
        repaired_sql, res = self.convention_repair(repaired_sql, gold_sql, db_id, res)
        
        repaired_sql, res = self.semantic_repair(repaired_sql, gold_sql, db_id, res)
        
        repaired_sql, res = self.output_align(repaired_sql, gold_sql, db_id, res)
        
        if self.llm.detect(repaired_sql, gold_sql, db_id, res):
            repaired_sql, usage = self.llm.repair_with_gpt(repaired_sql)
            # if not repaired_sql.executable:
            #     repaired_sql, res = self.syntax_repair(repaired_sql, gold_sql, db_id, origin_res)
        
        return repaired_sql.statement
    
    def query_detect_statistics(self, file_path:Path=None) -> None:
        detected_idx = set()
        false_detected_idx = set()
        for x_repairers in (
            self.syntax_repairers, 
            self.logic_repairers,
            self.convention_repairers,
            self.semantic_repairers,
            self.output_aligners
        ):
            for repairer in x_repairers:
                for idx, _, _, _, _ in repairer.false_detecting:
                    detected_idx.add(idx)
                    false_detected_idx.add(idx)
                for idx, _, _, _, _ in repairer.success_detected:
                    detected_idx.add(idx)
        
        for repairer in (self.nscr.anur, self.nscr.cser, self.nscr.mjr, self.nscr.mtcr):
            for idx, _, _, _, _ in repairer.false_detecting:
                detected_idx.add(idx)
                false_detected_idx.add(idx)
            for idx, _, _, _, _ in repairer.success_detected:
                detected_idx.add(idx)
        
        true_detected_idx = detected_idx - false_detected_idx
        print(f"total number of detected SQL queries: {len(detected_idx)}")
        print(f"total number of successfully detected SQL queries: {len(true_detected_idx)}")
        print(f"total number of false detected SQL queries: {len(false_detected_idx)}")
        print('+'*30)
        
        if file_path:
            write_json(file_path, {"All detection": len(detected_idx), "TP": len(true_detected_idx), "FP": len(false_detected_idx), "Overall": len(true_detected_idx)-len(false_detected_idx)})
    
    def error_detect_statistics(self, repairers:List=None, file_path:Path=None) -> None:
        """
        ...
        Args:
            repairers (List[Repairer]): 
        """
        x_repairers = [
            # self.aor, self.fhr,
            self.nscr.anur, self.nscr.cser, self.nscr.mjr, self.nscr.mtcr,  # no_such_column
            # self.mefr,
            self.dcr, self.er, self.nvr,
            self.dtfr, self.icr, self.iir, self.ijr, self.cr,
            # self.susr,
            # self.smr, self.ofhr, self.osr
        ] if repairers is None else repairers
        repairer_statistics = {}
        SUMUP = {"All detection": 0, "TP": 0, "FP": 0, "OVERALL": 0}
        for repairer in x_repairers:
            tp = len(repairer.success_detected)
            fp = len(repairer.false_detecting)
            overall = tp - fp
            detect_sum = tp + fp
            SUMUP['TP'] += tp
            SUMUP['FP'] += fp
            SUMUP['OVERALL'] += overall
            SUMUP['All detection'] += detect_sum
            repairer_statistics[type(repairer).__name__] = {"All detection": detect_sum, "TP": tp, "FP": fp, "OVERALL": overall}
            print(f"{type(repairer).__name__} -> TP: {tp}, FP: {fp}, OVERALL: {overall}, All detection: {detect_sum}")
        print("+++ SUM UP +++")
        pprint(SUMUP)
        repairer_statistics['SUMUP'] = {"All detection": SUMUP['All detection'], "TP": SUMUP['TP'], "FP": SUMUP['FP'], "OVERALL": SUMUP['OVERALL']}
        if file_path:
            write_json(file_path, repairer_statistics)
            
    def detection_statistics(self):
        pass
