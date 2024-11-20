import pandas as pd
from pathlib import Path

class_mapping = {
    'Syntax Error': ["Function Hallucination", "Missing Quote", "Misc_SE"],
    'Schema Related Execution Failure': ["Table-Column Mismatch", "Alias Not Use", "Schema Hallucination", "Not Qualified", "Misc_SREF"],
    'Logic Error': ["Implicit Type Conversion", "Using = instead of IN", "Ascending Sort with NULL", "Misc_LE"],
    'Convention Error': ["Value Specification", "Aggregation/Comparison Misuse", "Comparing Unrelated Columns", "Misc_CE"],
    'Semantic Error': ["Wrong COUNT Object", "Subquery Scope Inconsistency", "Missing DISTINCT", "Comparison Wrong Columns", "Projection Error", "ORDER BY Error", "Wrong Condition", "Table Selection Error", "Aggregation Structure Error", "Misc_SE"],
    'Not an Error': ["Output Format", "Gold Error", "FK Integrity", "Extreme Value Selection Ambiguity", "NULL Value in Output", "Misc_NAE"],
    'Others': ["Unclassifiable", "Misc_Others"],
}

index_mapping = {
    'Syntax Error': 0,
    'Schema Related Execution Failure': 1,
    'Logic Error': 2,
    'Convention Error': 3,
    'Semantic Error': 4,
    'Not an Error': 5,
    'Others': 6,
    'Correct': 7
}

def map_class_error(err:str) -> str:
    for k, v in class_mapping.items():
        if err in v:
            return k
    return 'Others'
def convert2class(source_column, target_column, source_res_column, target_res_column, df):
    # record:dict = {k:dict() for k in class_mapping.keys()}
    record:dict = {k1:{k2:0.0 for k2 in index_mapping.keys()} for k1 in index_mapping.keys()}
    both_cnt = 0
    source_cnt = 0
    target_cnt = 0
    for i, row in df.iterrows():
        source = row[source_column]
        target = row[target_column]
        source_res =int(row[source_res_column]) if row[source_res_column] == '1' or row[source_res_column] == '0' else row[source_res_column]
        target_res = row[target_res_column]
            
        if pd.isna(row[source_column]):
            source_class_name = 'Correct' if source_res == 1 else 'Others'
            source_cnt+=1
                
            if pd.isna(row[target_column]):
                target_class_name = 'Correct' if target_res == 1 else 'Others'
                # if source_class_tag != target_class_tag and target_class_tag == 'Correct':
                #     print(i, source, target, source_res, target_res)
                record[source_class_name][target_class_name] += 1.0
                continue
            
            for target_tag in [x.strip() for x in target.split('+')]:
                target_class_name = map_class_error(target_tag)
                    
                weight = 1.0 / len(target.split('+'))
                record[source_class_name][target_class_name] += weight
                
        elif pd.isna(row[target_column]):
            target_cnt+=1
            
            target_class_name = 'Correct' if target_res == 1 else 'Others'
                 
            try:
                for source_tag in [x.strip() for x in source.split('+')]:
                    weight = 1.0 / len(source.split('+'))
                    source_class_name = map_class_error(source_tag)
                    
                    weight = 1.0 / len(source.split('+'))
                    record[source_class_name][target_class_name] += weight
            except Exception as e:
                raise e
        else:
            source_tags = [x.strip() for x in source.split('+')]
            target_tags = [x.strip() for x in target.split('+')]
            # print(source_tags, target_tags)
            both_cnt+=1
            for source_tag in source_tags:
                weight = 1.0 / len(source_tags)
                source_class_name = map_class_error(source_tag)
                for target_tag in target_tags:
                    target_class_name = map_class_error(target_tag)
                    record[source_class_name][target_class_name] += weight

    print(f"source_cnt: {source_cnt}, target_cnt: {target_cnt}, both_cnt: {both_cnt}")
    return record

if __name__ == '__main__':
    file_path = Path('../data/chapter4/labeled_results/Effectiveness (MAC-SQL BIRD GPT-3.5-Turbo).xlsx')
    df = pd.read_excel(file_path)
    
    res = convert2class("Error_Type (Before Repair), Remapped", "Error_Type (LLM-Value), Remapped", "Result (Before Repair)", "Result (LLM-Value)", df)
    
    import json
    print(json.dumps(res, indent=4))
    
    value = [0.0] * 64
    
    for source_class, s_idx in index_mapping.items():
        for target_class, t_idx in index_mapping.items():
            value[s_idx * 8 + t_idx] = res[source_class][target_class]
    
    with open("sankey_metadata.json", 'w') as f:
        json.dump(value, f)
    
    