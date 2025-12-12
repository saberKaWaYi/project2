import os
import logging
from logging.handlers import RotatingFileHandler
import paramiko
import time
import atexit

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
                logging.error(e)
                time.sleep(1)
        else:
            logging.error(f"{self.hostname:<50s}{self.device_ip:<25s}{self.brand:<25s}链接失败。")
        return None

    def close(self):
        if self.client!=None:
            self.client.close()