from pathlib import Path
from MapleRepair.utils.format import read_json

TTFT = 1.047
ITL = 0.039
# TTFT and ITL is estimated by LLMPerf
# https://github.com/ray-project/llmperf

result_dir = Path("archived_result/MAC-SQL BIRD GPT-3.5-Turbo (@2024-11-01-21-58-15)")

db_overhead_dir = result_dir / "db_overhead"
total_overhead_dir = result_dir / "total_overhead"
llm_overhead_dir = result_dir / "llm_overhead"

overhead = {}

for idx in range(2000):
    overhead[idx] = {}
    db_overhead_path = db_overhead_dir / f"{idx}.json"
    if db_overhead_path.exists():
        overhead[idx]['db_overhead'] = read_json(db_overhead_path)
        ...
    llm_overhead_path = llm_overhead_dir / f"{idx}.json"
    if llm_overhead_path.exists():
        overhead[idx]['llm_overhead'] = read_json(llm_overhead_path)
        ...
    total_overhead_path = total_overhead_dir / f"{idx}.json"
    if total_overhead_path.exists():
        overhead[idx]['total_overhead'] = read_json(total_overhead_path)
        ...
    ...

_overhead = {} 
for k, v in overhead.items():
    if v:
        _overhead[k] = v
overhead = _overhead

total_overhead_sum = 0
total_llm_overhead_sum = 0
total_db_overhead_sum = 0
estimated_total_llm_overhead_sum = 0

for idx, log in overhead.items():
    # Calculate total_db_overhead
    total_db_overhead = sum(item['time'] for item in log['db_overhead']) if 'db_overhead' in log else 0
    total_db_overhead_sum += total_db_overhead
    
    db_invoke = len(log['db_overhead']) if 'db_overhead' in log else 0

    # Calculate total_llm_overhead
    total_llm_overhead = sum(item['time'] for item in log['llm_overhead']) if 'llm_overhead' in log else 0
    total_llm_overhead_sum += total_llm_overhead
    
    if 'llm_overhead' in log:
        for item in log['llm_overhead']:
            llm_input_tokens = item['llm_usage']['prompt_tokens']
            llm_output_tokens = item['llm_usage']['completion_tokens']
            estimated_total_llm_overhead = TTFT + llm_output_tokens * ITL
            estimated_total_llm_overhead_sum += estimated_total_llm_overhead
    
    llm_invoke = len(log['llm_overhead']) if 'llm_overhead' in log else 0
    
    total_overhead = log['total_overhead']['time']
    total_overhead_sum += total_overhead
    
    tool_overhead = total_overhead - total_llm_overhead - total_db_overhead
    total_tool_overhead_sum = total_overhead_sum - total_llm_overhead_sum - total_db_overhead_sum
    
    assert tool_overhead >= 0
    
    print(f"tool overhead: {tool_overhead}")
    print(f"llm overhead: {total_llm_overhead}, llm invoke: {llm_invoke}")
    print(f"db overhead: {total_db_overhead}, db invoke: {db_invoke}")
    print(f"total overhead: {total_overhead}")
    print(f"------------------------------------")

    ...

print(total_overhead_sum)
print(total_llm_overhead_sum)
print(estimated_total_llm_overhead_sum)
print(total_db_overhead_sum)
print(total_tool_overhead_sum)

...