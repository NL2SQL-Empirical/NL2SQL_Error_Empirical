# This script give out the number `1905`

# must in lower case!
valid_errmsg_prefix = {
    "Output Format".lower(),
    "Gold Error".lower(),
    "FK Integrity".lower(),
    "Extreme Value Selection Ambiguity".lower(),
    "NULL Value in Output".lower(),
}

import pandas as pd
from pathlib import Path

if __name__ == '__main__':
    data_root_dir = Path('data/chapter3/labeled_results')
    
    target_column_name = "Error_Type (Before Repair), Remapped"
    target_res_name = "Result (Before Repair)"
    
    total_contain_nae, total_pure_nae, total_true_err, total_gold_err = 0, 0, 0, 0
    
    for benchmark in ('BIRD', 'SPIDER'):
        benchmark_contain_nae, benchmark_pure_nae, benchmark_true_err, benchmark_gold_err = 0, 0, 0, 0
        for file_path in data_root_dir.glob("*.xlsx"):
            contain_nae, pure_nae, true_err, gold_err = 0, 0, 0, 0
            if benchmark not in file_path.name:
                continue
            df = pd.read_excel(file_path)
            
            df_dict = df.to_dict()
            
            for res, err in zip(df_dict[target_res_name].values(), df_dict[target_column_name].values()):
                if res not in (1, '1'):
                    contain_nae_flag = False
                    pure_nae_flag = False
                    if not pd.isna(err):
                        err = err.lower()
                        if 'gold error' in err:
                            gold_err += 1
                        for nae in valid_errmsg_prefix:
                            if nae == err:
                                # only nae in err
                                contain_nae_flag = True
                                pure_nae += 1
                                contain_nae += 1
                                break
                            elif nae in err:
                                # contain nae, also other errors
                                contain_nae_flag = True
                                pure_nae_flag = True
                                contain_nae += 1
                                break
                    if not pure_nae_flag:
                        true_err += 1
            
            print(f"{file_path} contains {contain_nae} queries have not-a-error problem, in which {true_err} of them have at least one real error.")
            benchmark_contain_nae += contain_nae
            benchmark_pure_nae += pure_nae
            benchmark_true_err += true_err
            benchmark_gold_err += gold_err
            
        print(f"Results in {benchmark} contain {benchmark_contain_nae} queries have not-a-error problem, in which {benchmark_true_err} of them have at least one real error.")
        total_contain_nae += benchmark_contain_nae
        total_pure_nae += benchmark_pure_nae
        total_true_err += benchmark_true_err
        total_gold_err += benchmark_gold_err
        
    print(f"All results contain {total_contain_nae} queries have not-a-error problem, in which {total_true_err} of them have at least one real error.")
    ...