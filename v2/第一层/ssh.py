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

handler=get_rotating_handler("ssh.log")
logging_ssh=logging.getLogger("ssh")
logging_ssh.setLevel(logging.INFO)
logging_ssh.addHandler(handler)

import atexit
import paramiko
import time

class SSH_Server:

    def __init__(self,hostname,device_ip,brand):
        self.hostname=hostname
        self.device_ip=device_ip
        self.brand=brand
        self.client=self.login()
        atexit.register(self.close)

    def login(self):
        client=paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        for i in range(5):
            try:
                client.connect(
                    hostname=self.device_ip,
                    username="root",
                    password="P@$$w0rd"
                )
                return client
            except Exception as e:
                time.sleep(1)
        else:
            logging_ssh.error(f"{self.hostname:<50s}{self.device_ip:<25s}{self.brand:<25s}登录失败。")
        return None

    def close(self):
        if self.client!=None:
            self.client.close()
            self.client=None