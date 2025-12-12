import os
import logging
from logging.handlers import RotatingFileHandler
from pymongo import MongoClient
import time
import atexit
import pandas as pd
from pymysql import connect
from pymysql.cursors import DictCursor
from clickhouse_driver import Client

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

class Connect_Mongodb:

    def __init__(self,config):
        self.config=config
        self.client=self.login()
        self.db=self.get_database()
        atexit.register(self.close)

    def login(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                client=MongoClient(host=self.config["mongodb"]["HOST"],port=self.config["mongodb"]["PORT"])
                client.cds_cmdb.authenticate(self.config["mongodb"]["USERNAME"],self.config["mongodb"]["PASSWORD"])
                return client
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("mongodb登录失败。")
        raise Exception("mongodb登录失败。")
    
    def get_database(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                return self.client.get_database("cds_cmdb")
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("cds_cmdb获取失败。")
        raise Exception("cds_cmdb获取失败。")
    
    def close(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                self.client.close()
                return
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("mongodb关闭失败。")
        raise Exception("mongodb关闭失败。")

    def get_collection(self,name,condition1,condition2):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                data=pd.DataFrame(self.db.get_collection(name).find(condition1,condition2)).astype(str)
                return data
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error(f"{name}数据获取失败。")
        raise Exception(f"{name}数据获取失败。")

class Connect_Mysql:

    def __init__(self,config):
        self.config=config
        self.client=self.login()
        atexit.register(self.close)

    def login(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                if self.config["mysql"].get("DATABASE",None)==None:
                    client=connect(host=self.config["mysql"]["HOST"],port=self.config["mysql"]["PORT"],user=self.config["mysql"]["USERNAME"],password=self.config["mysql"]["PASSWORD"],database="nebula_graph",charset="utf8",cursorclass=DictCursor)
                else:
                    client=connect(host=self.config["mysql"]["HOST"],port=self.config["mysql"]["PORT"],user=self.config["mysql"]["USERNAME"],password=self.config["mysql"]["PASSWORD"],database=self.config["mysql"]["DATABASE"],charset="utf8",cursorclass=DictCursor)
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
    
class Connect_Clickhouse:

    def __init__(self,config):
        self.config=config
        self.client=self.login()
        atexit.register(self.close)

    def login(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                client=Client(host=self.config["clickhouse"]["HOST"],port=self.config["clickhouse"]["PORT"],user=self.config["clickhouse"]["USERNAME"],password=self.config["clickhouse"]["PASSWORD"])
                return client
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("clickhouse登录失败。")
        raise Exception("clickhouse登录失败。")
    
    def close(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                self.client.disconnect()
                return
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("clickhouse关闭失败。")
        raise Exception("clickhouse关闭失败。")
    
    def query(self,query):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                data,columns=self.client.execute(query,with_column_types=True)
                columns=[col[0] for col in columns]
                data=pd.DataFrame(data,columns=columns).astype(str)
                return data
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error(f"{query}数据获取失败。")
        raise Exception(f"{query}数据获取失败。")