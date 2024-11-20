from MapleRepair.Database import Database, DBs
from MapleRepair.config import dataset, result_root_dir, generalizability_test, data_split
from tqdm import tqdm
from pathlib import Path
from MapleRepair.utils.format import write_json, read_json
import time
from MapleRepair.utils.persistence import make_log
import json
from gold_err import gold_err_idx
from MapleRepair.MapleRepair import MapleRepair
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--result_path", type=str, required=True)
    parser.add_argument("--before", action="store_true")
    parser.add_argument("--after", action="store_true")
    parser.add_argument("--LLMdisable", action="store_true")
    args = parser.parse_args()
    before_flag = args.before
    after_flag = args.after
    assert before_flag or after_flag
    print(args)

    result_path = Path(args.result_path)
    
    positive_repairing = 0
    false_repairing = 0
    
    final_repaired_result = []
    
    results_list = read_json(result_path)
    
    R = MapleRepair(result_root_dir=result_root_dir, LLM_enable=args.LLMdisable)
    
    exception_dict = {}
    
    actual_incorrect_sql = 0
    
    for result in tqdm(results_list):
        db_id = result['db_id']
        db:Database = DBs[db_id]
        question = result['question']
        evidence = result['evidence']
        
        idx = result['idx']
        gold_sql = result['gold']
        if before_flag:
            sql_statement = result['pred']
            res = result['pred_result']
        elif after_flag:
            sql_statement = result['repair_sql']
            res = result['repair_result']
            
        if not generalizability_test:
            if idx in gold_err_idx:
                continue
            
        if dataset == 'SPIDER' and data_split == 'DEV':
            if db_id == 'flight_2':
                continue
            
        if res not in (1, '1'):
            actual_incorrect_sql += 1
        
        try:
            start = time.perf_counter()
            repaired_sql_statement = R.repair(sql_statement, gold_sql=gold_sql, db_id=db_id, origin_res=res, question_id=idx, question=question, evidence=evidence)
        except BaseException as be:
            if type(be).__name__ not in exception_dict:
                exception_dict[type(be).__name__] = []
            exception_dict[type(be).__name__].append((idx, str(be)))
            continue
        finally:
            end = time.perf_counter()
            # print(f"Time taken: {end - start:0.9f} seconds")
            log_path = result_root_dir / 'total_overhead' / f"{idx}.json"
            content = json.dumps({"time": end-start})
            make_log(log_path, content)
            
        # repaired_sql_statement = R.repair(sql_statement, gold_sql=gold_sql, db_id=db_id, origin_res=res, question_id=idx, question=question, evidence=evidence)
        
        repaired_res, err_msg = db.execution_match(repaired_sql_statement, gold_sql)
        if res == 1 and repaired_res != 1:
            false_repairing += 1
        elif res != 1 and repaired_res == 1:
            positive_repairing += 1
        result['repair_sql'] = repaired_sql_statement
        result['repair_result'] = repaired_res if err_msg is None else err_msg
        final_repaired_result.append(result)
        
    print(f"The number of actual incorrect SQL query: {actual_incorrect_sql}")
    if before_flag:
        R.query_detect_statistics(file_path=R.result_root_dir /"query_statistics (before).json")
        R.error_detect_statistics(file_path=R.result_root_dir /"error_statistics (before).json")
    elif after_flag:
        R.query_detect_statistics(file_path=R.result_root_dir /"query_statistics (before).json")
        R.error_detect_statistics(file_path=R.result_root_dir /"error_statistics (before).json")
        
    write_json(R.result_root_dir / 'repaired_results.json', final_repaired_result)
    # print(f"llm_call: {R.llm.llm_call}")
    # print(positive_repairing)
    # print(false_repairing)
    # pprint(exception_dict)
    write_json(R.result_root_dir / 'exception_dict.json', exception_dict)