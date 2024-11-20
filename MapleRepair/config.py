#TODO: Use yaml file to store the configuration

import os
import sys
from dotenv import load_dotenv
import logging
import logging.config
import colorlog
from datetime import datetime
from pathlib import Path

load_dotenv('./project.env')

DATASET_DB_PATHS = {
    'DEV_BIRD': 'DEV_BIRD_DB_ROOT_PATH',
    'TRAIN_BIRD': 'TRAIN_BIRD_DB_ROOT_PATH',
    'DEV_SPIDER': 'DEV_SPIDER_DB_ROOT_PATH',
    'TEST_SPIDER': 'TEST_SPIDER_DB_ROOT_PATH',
}
dataset = os.getenv('DATASET')
if dataset not in ('BIRD', 'SPIDER'):
    raise ValueError(f"Unsupported dataset! Dataset must be one of {('BIRD', 'SPIDER')}")
data_split = os.getenv('DATA_SPLIT')
if data_split not in ('TRAIN', 'DEV', 'TEST'):
    raise ValueError(f"Unsupported data_split! Data_split must be one of {('TRAIN', 'DEV', 'TEST')}")

db_root_path = os.getenv(DATASET_DB_PATHS[f"{data_split}_{dataset}"])
print(f"db_root_path: {db_root_path}")

openai_api_key = os.getenv('OPENAI_API_KEY')
openai_base_url = os.getenv('OPENAI_BASE_URL')

default_model = os.getenv('DEFAULT_MODEL')
print(f"default_model: {default_model}")

db_cache_dir = os.getenv('DB_CACHE_DIR')
print(f"db_cache_dir: {db_cache_dir}")

result_root_dir = Path('results') / datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
os.makedirs(result_root_dir, exist_ok=True)

generalizability_settings = [
    {'DATASET': 'BIRD', 'DATA_SPLIT': 'TRAIN'},
    {'DATASET': 'SPIDER', 'DATA_SPLIT': 'TEST'}
]

generalizability_test = any(
    setting['DATASET'] == dataset and setting['DATA_SPLIT'] == data_split
    for setting in generalizability_settings
)

print(f"generalizability_test: {generalizability_test}")
debugging = False
print(f"debugging: {debugging}")
evaluation = False
print(f"evaluation: {evaluation}")
parallel_init = True
print(f"parallel_init: {parallel_init}")
persistence = True
print(f"persistence: {persistence}")

DBs_name = [p.name for p in Path(db_root_path).iterdir() if p.is_dir()]

#NOTE: The ORDER Matters

ISO_DATE_FORMATS = [
    # standard date format
    (r'(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}):(\d{2}).(\d{3})', '%Y-%m-%d %H:%M:%S.%f'), # YYYY-MM-DD HH:MM:SS.SSS
    (r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).(\d{3})', '%Y-%m-%dT%H:%M:%S.%f'),  # YYYY-MM-DDTHH:MM:SS.SSS
    (r'(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}):(\d{2})', '%Y-%m-%d %H:%M:%S'),            # YYYY-MM-DD HH:MM:SS
    (r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})', '%Y-%m-%dT%H:%M:%S'),             # YYYY-MM-DDTHH:MM:SS
    (r'(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2})', '%Y-%m-%d %H:%M'),                       # YYYY-MM-DD HH:MM
    (r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})', '%Y-%m-%dT%H:%M'),                        # YYYY-MM-DDTHH:MM
    (r'(\d{4})-(\d{2})-(\d{2})', '%Y-%m-%d'),                                              # YYYY-MM-DD
]

USER_DEFINED_DATE_FORMATS = [
    # non-standard date format(self-defined)
    (r'(\d{4})(\d{2})(\d{2})\s(\d{2})(\d{2})(\d{2})', '%Y%m%d %H%M%S'),                    # YYYYMMDD HHMMSS
    (r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})', '%Y-%m-%dT%H:%M'),                        # YYYY-MM-DDTHH:MM
    (r'(\d{2})-(\d{2})-(\d{4})\s(\d{2}):(\d{2})', '%d-%m-%Y %H:%M'),                       # DD-MM-YYYY HH:MM
    (r'(\d{2})-(\d{2})-(\d{2})', '%y-%m-%d'),                                              # YY-MM-DD
    (r'(\d{2})/(\d{2})/(\d{4})', '%m/%d/%Y'),                                              # MM/DD/YYYY
    (r'(\d{4})/(\d{2})/(\d{2})', '%Y/%m/%d'),                                              # YYYY/MM/DD
    (r'(\d{2})-(\d{2})', '%m-%d'),                                                         # MM-DD
    # (r'(\d{8})', '%Y%m%d'),                                                                # YYYYMMDD[To Be Disscussed]
]

DATE_FORMATS = ISO_DATE_FORMATS + USER_DEFINED_DATE_FORMATS

TIME_FORMATS = [
    # standard time format
    (r'(\d{2}):(\d{2}):(\d{2}).(\d{3})', '%H:%M:%S.%f'),  # HH:MM:SS.SSS
    (r'(\d{2}):(\d{2}):(\d{2})', '%H:%M:%S'),             # HH:MM:SS
    (r'(\d{2}):(\d{2})', '%H:%M'),                        # HH:MM

    # non-standard time format(self-defined)
    (r'(\d{1}):(\d{2}).(\d{3})', '%M:%S.%f'),             # M:SS.SSS
    (r'(\d{1}):(\d{2})', '%M:%S'),                        # M:SS
    # (r'(\d{6})', '%H%M%S'),                               # HHMMSS[To Be Disscussed]
]
