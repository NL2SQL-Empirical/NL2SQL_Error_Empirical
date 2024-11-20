from concurrent.futures.process import ProcessPoolExecutor
from func_timeout import func_set_timeout, FunctionTimedOut
import sqlite3
from sqlite3 import OperationalError
from concurrent.futures import as_completed
from pathlib import Path
from MapleRepair.utils.format import read_json, write_json
from MapleRepair.config import dataset, data_split, db_root_path, generalizability_test
from gold_err import gold_err_idx
import argparse

TIMEOUT = 120

@func_set_timeout(TIMEOUT)
def _evaluate_sql(predicted_sql, ground_truth, db_path):
    # Connect to the database
    conn = sqlite3.connect(db_path)
    conn.text_factory = lambda b: b.decode(errors="ignore")  # avoid gbk/utf8 error, copied from sql-eval.exec_eval
    cursor = conn.cursor()
    cursor.execute(predicted_sql)
    predicted_res = cursor.fetchall()
    cursor.execute(ground_truth)
    ground_truth_res = cursor.fetchall()
    res = 0
    # todo: this should permute column order!
    if set(predicted_res) == set(ground_truth_res):
        res = 1
    return res

def evaluate_sql(predicted_sql, ground_truth, db_path):
    try:
        res = _evaluate_sql(predicted_sql, ground_truth, db_path)
    except OperationalError as oe:
        res = str(oe)
    except FunctionTimedOut as TLE:
        print('timeout')
        res = 'timeout'
    except Exception as e:
        res = str(e)
    return res

def parallel_evaluate(pairs):
    """
    Args:
        pairs (List): [(pred, gold, db_path), ...]
    Returns:
        List: [(id, res), ...]
            res := 0, 1, errmsg
            id is not question_id!!!
    """
    with ProcessPoolExecutor(max_workers=128) as executor:
        results = []
        future_to_db_id = {executor.submit(evaluate_sql, pred, gold, db_path): idx for idx, (pred, gold, db_path) in enumerate(pairs)}
        for future in as_completed(future_to_db_id):
            try:
                idx = future_to_db_id[future]
                res = future.result()
                results.append((idx, res))
            except Exception as e:
                print(str(e))
    results = sorted(results, key=lambda x:x[0])
    return results

def repair_statisics(path:Path):
    """
    Report repair statisics (TP, FP, Overall) from normalized json file.
    Args:
        path (Path): path of normalized json file
    Returns:
        (TP, FP, OVERALL), OVERALL=TP-FP
    """
    js = read_json(path)
    
    INCORRECT_SQL_NUMBER = 0
    TP, FP, OVERALL = 0, 0, 0
    for item in js:
        if generalizability_test:
            idx = item['idx']
            if idx in gold_err_idx:
                continue
            db_id = item['db_id']
            if dataset == 'SPIDER':
                if db_id == "flight_2":
                    continue
        if item['pred_result'] in (1, '1') and item['repair_result'] not in (1, '1'):
            FP += 1
        elif item['pred_result'] not in (1, '1'):
            INCORRECT_SQL_NUMBER += 1
            if item['repair_result'] in (1, '1'):
                TP += 1
    OVERALL = TP - FP
    print(TP, FP, OVERALL)
    return TP, FP, OVERALL

if __name__ == '__main__':    
    parser = argparse.ArgumentParser()
    parser.add_argument("--result_path", type=str, required=True)
    args = parser.parse_args()

    result_path = Path(args.result_path)
    # result_path = Path("")
    
    js = read_json(result_path)
    
    pairs = []
    for i, item in enumerate(js):
        query = item['repair_sql']
        gold = item['gold']
        db_id = item['db_id']
        db_path = Path(db_root_path) / f"{db_id}/{db_id}.sqlite"
        pairs.append((query, gold, db_path))
    parallel_return = parallel_evaluate(pairs)
    
    assert len(pairs) == len(js)
    filled_js = []
    for i, item in enumerate(js):
        item['repair_result'] = parallel_return[i][1]
        filled_js.append(item)
    
    write_json(result_path, filled_js)
    
    repair_summary = {"Successful repair": 0, "False repair": 0, "Overall": 0}
    repair_summary['Successful repair'], repair_summary['False repair'], repair_summary['Overall'] = repair_statisics(result_path)
    dir = result_path.parent
    summary_path = dir / "repair_summary.json"
    if not summary_path.exists():
        write_json(summary_path, repair_summary)