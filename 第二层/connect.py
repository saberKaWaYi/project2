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

handler=get_rotating_handler("mysql.log")
logging_connect_mysql=logging.getLogger("mysql")
logging_connect_mysql.setLevel(logging.INFO)
logging_connect_mysql.addHandler(handler)

handler=get_rotating_handler("nebula.log")
logging_nebula=logging.getLogger("nebula")
logging_nebula.setLevel(logging.INFO)
logging_nebula.addHandler(handler)

import atexit
import time
import pandas as pd

from pymysql import connect
from pymysql.cursors import DictCursor

class Connect_Mysql:

    def __init__(self,config):
        self.config=config
        self.client=self.login()
        atexit.register(self.close)

    def login(self):
        for _ in range(self.config["connection"]["TIMES"]):
            try:
                client=connect(host=self.config["mysql"]["HOST"],port=self.config["mysql"]["PORT"],user=self.config["mysql"]["USERNAME"],password=self.config["mysql"]["PASSWORD"],charset="utf8",cursorclass=DictCursor)
                return client
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("mysql登录失败。")
        raise Exception("mysql登录失败。")
    
    def close(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                self.client.close()
                return
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("mysql关闭失败。")
        raise Exception("mysql关闭失败。")
    
    def get_table_data(self,table_name,query):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                with self.client.cursor() as cursor:
                    cursor.execute(query)
                    columns=[desc[0] for desc in cursor.description]
                    data=cursor.fetchall()
                    data=pd.DataFrame(data,columns=columns).astype(str)
                    return data
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error(f"{table_name}数据获取失败。")
        raise Exception(f"{table_name}数据获取失败。")
    
from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool
    
class Connect_Nebula:

    def __init__(self,config):
        self.config1=config["connection"]
        self.config2=config["nebula"]
        self.connectionpool=None
        self.client=None

    def open_nebula(self):
        for i in range(self.config1["TIMES"]):
            try:
                config=Config();config.min_connection_pool_size=self.config2["MIN_CONNECTION_POOL_SIZE"];config.max_connection_pool_size=self.config2["MAX_CONNECTION_POOL_SIZE"]
                connectionpool=ConnectionPool();connectionpool.init([(self.config2["HOST"],self.config2["PORT"])],config)
                client=connectionpool.get_session("root","nebula")
                self.connectionpool=connectionpool;self.client=client
                return
            except:
                time.sleep(self.config1["TIME"])
        logging.error("nebula链接失败。")
        raise Exception("nebula链接失败。")
 
    def close_nebula(self):
        for i in range(self.config1["TIMES"]):
            try:
                self.client.release()
                self.connectionpool.close()
                return
            except:
                time.sleep(self.config1["TIME"])
        logging.error("nebula关闭失败。")
        raise Exception("nebula关闭失败。")