from pathlib import Path
from typing import Any

def make_log(log_path:Path, content:Any) -> None:
    dir_path = log_path.parent
    dir_path.mkdir(parents=True, exist_ok=True)
    log_path.touch()
    log_path.write_text(content)