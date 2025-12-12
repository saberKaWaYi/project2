import os

log_dir="logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

import logging
from logging.handlers import RotatingFileHandler

def get_rotating_handler(filename,max_bytes=1024*1024*1024,backup_count=5):
    handler=RotatingFileHandler(
        os.path.join(log_dir,filename),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    formatter=logging.Formatter(
        '%(asctime)s - %(levelname)s - %(module)s.%(funcName)s - %(message)s'
    )
    handler.setFormatter(formatter)
    return handler

handler=get_rotating_handler("main.log")
logging_main=logging.getLogger("main")
logging_main.setLevel(logging.INFO)
logging_main.addHandler(handler)

import sys
from pathlib import Path
import subprocess

def run_script(script_path):
    script_file=Path(script_path)
    if not script_file.exists():
        logging_main.error(f"{script_path}找不到。")
        return False
    result=subprocess.run(
        [sys.executable,str(script_file)],
        check=True
    )
    if result.returncode!=0:
        logging_main.error(f"{script_path}失败了。")
        return False
    logging_main.info(f"{script_path}成功了。")
    return True

SCRIPTS=[
    "physical_relationship.py",
    "nic1.py",
    "nic2.py",
    "between_interface_and_interface.py",
    "between_interface_and_nic_preparation1.py",
    "between_interface_and_nic_preparation2.py",
    "between_interface_and_nic.py"
]

def main():
    for script in SCRIPTS:
        if not run_script(script):
            sys.exit(1)

if __name__=="__main__":
    main()