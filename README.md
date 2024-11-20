# Demystifying and Repairing In-Context-Learning-Based Text-to-SQL Errors

Artifact repository for the paper [_Demystifying and Repairing In-Context-Learning-Based Text-to-SQL Errors_]().

---

## MapleRepair
An automated error detection and repairing tool for text-to-SQL tasks, with minimal mis-repairs. 

### Install
This repository contains the source code for reproducing the results in our paper. Please start by cloning this repository:
```
git clone https://github.com/NL2SQL-Empirical/NL2SQL_Error_Empirical
```

We recommend using conda as virtual environment manager for running the scripts. You can create a virtual environment in conda using the following command:
```
conda create -n nl2sql_empirical python=3.10
```

After creating the virtual environment, you can activate it using the following command:
```
conda activate nl2sql_empirical
```

To install all software dependencies, please execute the following command:

```
docker run -d --name SQLRepair -p 6333:6333 \
    -v $(pwd)/qdrant_storage:/qdrant/storage:z \
    qdrant/qdrant:v1.11.1
```

```
conda install onnxruntime=1.17.1=py310hf70ce4d_0
```

```
pip install -r requirements.txt
```

As for hardware dependencies, 32 GB of memory is enough.

### Data Preparation

#### Start from data we provided (***Recommended, Faster than starting from scratch!!!***)

1. Download sampled benchmarks: [BIRD](https://drive.google.com/file/d/198NtXgoGuRn9_70ITHTiPVZqNIb_gssM/view?usp=sharing) and [SPIDER](https://drive.google.com/file/d/1kslOOk9I2-ptRDxIHpnZAdLTRn0wg5Qy/view?usp=sharing)
   
   ***You should use sampled benchmarks provided by us!*** OR you should follow the instruction of ***start from scratch***
2. Download [vector database data for MapleRepair](https://drive.google.com/file/d/103b9TpIFOIBdSn0NHzCvbl5AfxJlq2MX/view?usp=sharing)
3. Download [database cache for MapleRepair](https://drive.google.com/file/d/104xG4CmYxfKWvX6aVIuEMmze1WkDGptc/view?usp=sharing)
4. Unzip and organize them as follows:
```
NL2SQL_ERROR_EMPIRICAL
├── .cache          # database cache
│   ├── QklSRA==
│   └── U1BJREVS
├── qdrant_storage  # vector database
│   ├── aliases
│   ├── collections
│   └── raft_state.json
├── benchmarks      # benchmarks (BIRD, SPIDER)
│   ├── bird
│   │   ├── dev
│   │   │   ├── dev_databases
│   │   │   ├── dev.json
│   │   │   ├── dev.sql
│   │   │   ├── dev_tables.json
│   │   │   └── dev_tied_append.json
│   │   └── train
│   │       ├── train_databases
│   │       ├── train_gold.sql
│   │       ├── train.json
│   │       └── train_tables.json
│   └── spider
│       ├── dev
│       │   ├── database
│       │   ├── dev_gold.sql
│       │   ├── dev.json
│       │   └── tables.json
│       └── test
│           ├── database
│           ├── dev_gold.sql
│           ├── dev.json
│           └── tables.json
└── ...
```

#### Start from scratch (TBD.)

### Configuration
For doing translation with a model and dataset, first you need to make a copy of `project.env.template` and name it as `project.env` file in src/MapleRepair and make necessary modification (e.g. enter your `OPENAI_API_KEY`).

### Evaluation
```bash
python main.py <options>
```
1. --result_path: path to results.json
2. --before: repair queries before its repair (in key `pred`)
3. --after: repair queries after its repair (in key `repair_sql`)
4. --LLMdisable: disable LLM, completely rule-based repair.

An example:
```bash
python main.py --result_path 'ICL_results/dev/bird/MAC-SQL BIRD GPT-3.5-Turbo.json' --before --LLMdisable
```

Evaluation results will be stored in `results/<%Y-%m-%d-%H-%M-%S>` dir.

After MapleRepair finished, run the script to evaluate the correctness of repaired SQL queries.
```bash
python data/utils/parallel_evaluation.py --result_path <result json file>
```

### Results
1. Download logs and results of MapleRepair: [logs & results](https://drive.google.com/file/d/1VRkZbvFyE7MIMNMQQn4eud2WeXM21OGV/view?usp=sharing).
   
2. Unzip and organize them as follows:
```
NL2SQL_ERROR_EMPIRICAL
├── data
│   ├── chapter5
│   │   ├── MapleRepair
│   │   └── generalizability
│   │       ├── BIRD Sampled_train
│   │       │   └── <Technique> <Benchmark> <LLM>.json
│   │       └── SPIDER Sampled_test
│   │           └── <Technique> <Benchmark> <LLM>.json
│   └── ...
└── ...
```

### Uniform result json format
```json
[
    {
        "idx": question_id,
        "db_id": database_id,
        "question": question,
        "evidence": evidence,
        "gold": ground-truth,
        "pred": SQL query before repair,
        "pred_result": the result (correctness) of pred,
        "repair_sql": SQL query after repair,
        "repair_result": the result (correctness) of repair_sql
    },
    ...
]
```
\* MapleRepair always store the repaired SQL query and its result into `repair_sql` and `repair_result`! When option `--after` is enabled, `repair_sql` and `repair_result` will be ***overwritten***!

### Usage
```python
from MapleRepair import MapleRepair
R = MapleRepair()
pred_sql = NL2SQL(question, db_id)
repair_sql = R.repair(pred_sql, db_id)
```

## Empirical Results
The empirical results is organized as following directory structure:
```
NL2SQL_ERROR_EMPIRICAL
├── data
│   ├── chapter3
│   │   └── labeled_results
│   │       └── <Technique> <Benchmark> <LLM>.xlsx
│   ├── chapter4
│   │   └── labeled_results
│   │       └── Effectiveness (MAC-SQL BIRD GPT-3.5-Turbo).xlsx
│   └── ...
└── ...
```

The description of each file is as follows:

1. \<Technique\> \<Benchmark> \<LLM>.xlsx

2. Effectiveness (MAC-SQL BIRD GPT-3.5-Turbo).xlsx

### Scripts
We provide scripts for reproducing our results in this work. 

1. generate sankey figure (Figure 11 in paper).
```bash
cd sankey
bash sankey_generate.sh  # execute this when you are in sankey!
```
Generated sankey figures are stored in sankey/images.