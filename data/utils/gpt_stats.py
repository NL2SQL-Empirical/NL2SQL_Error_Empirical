import pandas as pd
from maps import re_mapping
from pathlib import Path

class ErrorStatistics:
    def __init__(self, mapping):
        self.mapping = mapping
        self.reset_stats()

    def reset_stats(self):
        """Initialize or reset the error statistics."""
        self.err_stats = {k: 0 for k in self.mapping.keys()}
        self.err_stats['Unclassifiable'] = 0
        self.err_stats['Gold Error'] = 0

    def update_stats(self, error_list):
        """Update statistics based on a list of errors."""
        if not error_list:
            self.err_stats['Unclassifiable'] += 1
            return

        for err in error_list:
            normalized_err = err.strip()
            if normalized_err == 'Misc_SE':
                ...
            if 'Gold Error' in normalized_err:
                self.err_stats['Gold Error'] += 1
                continue
            
            for key in self.mapping.keys():
                if key.lower() == normalized_err.lower():
                    self.err_stats[key] += 1

    def get_stats_series(self, length):
        """Return a pandas Series of the current stats, extended to match DataFrame length."""
        series = pd.Series(self.err_stats.values(), dtype=int)
        return series.reindex(range(length), fill_value=None)

def is_correct(result: str) -> bool:
    """Check if the result indicates a correct repair."""
    try:
        return int(result) == 1
    except (ValueError, TypeError):
        return False

def process_file(file_path, original_column, res_column_name, target_column_name, mapping):
    """Process a single Excel file to update error statistics."""
    df = pd.read_excel(file_path)
    
    if original_column not in df.columns:
        return
    if res_column_name not in df.columns:
        return
    
    print(f"Processing {file_path}")
    
    error_stats = ErrorStatistics(mapping)

    for _, row in df.iterrows():
        res = row.get(res_column_name)
        if not is_correct(res):
            error = row.get(original_column, '')
            if not error or pd.isna(error):
                error_list = []
            else:
                error_list = error.split('+')
            error_stats.update_stats(error_list)
    
    df[target_column_name] = error_stats.get_stats_series(df.shape[0])
    df.to_excel(file_path, index=False)
    print(f"Completed processing {file_path}")

if __name__ == '__main__':
    columnsXres = [
        ('Error_Type (Before Repair)', 'Result (Before Repair)'),
        ('Error_Type (After Repair)', 'Result (After Repair)'),
        ('Error_Type (Rule-Exe)', 'Result (Rule-Exe)'),
        ('Error_Type (LLM-Plain)', 'Result (LLM-Plain)'),
        ('Error_Type (LLM-Exe)', 'Result (LLM-Exe)'),
        ('Error_Type (LLM-Value)', 'Result (LLM-Value)'),
        ('Error_Type (LLM-Extr)', 'Result (LLM-Extr)')
    ]
    
    dir_paths = [
        Path('../chapter3/labeled_results'),
        Path('../chapter4/labeled_results'),
    ]

    for dir_path in dir_paths:
        for column, res in columnsXres:
            column = f'{column}, Remapped'
            result_column_name = f"Error_Count ({column})"
            for file_path in dir_path.glob("*.xlsx"):
                process_file(file_path, column, res, result_column_name, re_mapping)
