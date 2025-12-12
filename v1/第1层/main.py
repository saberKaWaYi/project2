import os
import logging
from logging.handlers import RotatingFileHandler
import sys
from pathlib import Path
import subprocess

log_dir="logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            os.path.join(log_dir,"app.log"),
            maxBytes=1024*1024*1024,
            backupCount=5
        ),
        logging.StreamHandler()
    ]
)

def run_script(script_path):
    script_file=Path(script_path)
    if not script_file.exists():
        logging.error(f"{script_path}找不到。")
        return False
    result=subprocess.run(
        [sys.executable,str(script_file)],
        check=True
    )
    if result.returncode!=0:
        logging.error(f"{script_path}失败了。")
        return False
    logging.info(f"{script_path}成功了。")
    return True

SCRIPTS=[
    "create_table.py",
    "data_migration.py",
    "data_collection1.py",
    "data_collection2_1_1.py",
    "data_collection2_1_2.py",
    "data_collection2_2.py"
]

def main():
    for script in SCRIPTS:
        if not run_script(script):
            sys.exit(1)

if __name__=="__main__":
    main()