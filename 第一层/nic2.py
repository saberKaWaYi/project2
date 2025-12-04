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

handler=get_rotating_handler("nic2.log")
logging_nic2=logging.getLogger("nic2")
logging_nic2.setLevel(logging.INFO)
logging_nic2.addHandler(handler)

from connect import Connect_Mysql
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor,as_completed
from ssh import SSH_Server

class Run:

    def __init__(self,config):
        self.config=config
        self.db_mysql=Connect_Mysql(self.config)
        self.db_mysql_client=self.db_mysql.client.cursor()
        self.result1=[];self.lock1=threading.Lock()
        self.result2=[];self.lock2=threading.Lock()
        self.result3=[];self.lock3=threading.Lock()
        self.time=datetime.now()

    def fc(self,hostname,ip,brand):
        if "." not in ip:
            with self.lock3:
                self.result3.append((hostname,ip,brand,"ip有错误",self.time,"nic2.py"))
            logging_nic2.error(f"{hostname},{ip},{brand}。ip有错误。")
            return
        for i in ["SDS","MDM","EBS"]:
            if i in hostname:
                break
        else:
            with self.lock3:
                self.result3.append((hostname,ip,brand,"放弃处理",self.time,"nic2.py"))
            logging_nic2.error(f"{hostname},{ip},{brand}。放弃处理。")
            return
        client=SSH_Server(hostname,ip,brand).client
        if client==None:
            with self.lock3:
                self.result3.append((hostname,ip,brand,"登录不上",self.time,"nic2.py"))
            logging_nic2.error(f"{hostname},{ip},{brand}。登录不上。")
            return
        stdin,stdout,stderr=client.exec_command("ip addr show",timeout=60)
        output=stdout.read().decode('utf-8').strip()
        for line in output.split("\n"):
            if not line:
                continue
            if line[0]==" ":
                continue
            line=line[line.index(":")+1:]
            line=line[:line.index(":")].strip()
            self.result1.append((hostname,ip,brand,line,"","存储",""))

    def collect(self):
        data=self.db_mysql.get_table_data("","select hostname,in_band_ip,brand2 from topu.server where server_group='云平台存储' or server_group='裸金属存储'")[["hostname","in_band_ip","brand2"]].values.tolist()
        with ThreadPoolExecutor(max_workers=50) as executor:
            pool=[]
            for i in data:
                pool.append(executor.submit(self.fc,i[0],i[1],i[2]))
            for task in as_completed(pool):
                task.result()

    def insert_data(self):
        sql='''
        insert into topu.nic (hostname,ip,brand,name,mac_address,type,description) values (%s,%s,%s,%s,%s,%s,%s)
        '''
        self.db_mysql_client.executemany(sql,self.result1)
        self.db_mysql.client.commit()
        sql='''
        insert into topu.between_server_and_nic (hostname,name) values (%s,%s)
        '''
        self.db_mysql_client.executemany(sql,[(i[0],i[3]) for i in self.result1])
        self.db_mysql.client.commit()
        sql='''
        insert into cds_report.collect_lldp_from_server (hostname,ip,brand,info,time,file) values (%s,%s,%s,%s,%s,%s)
        '''
        self.db_mysql_client.executemany(sql,self.result3)
        self.db_mysql.client.commit()

    def run(self):
        self.collect()
        self.insert_data()

if __name__=="__main__":
    config={
        "connection":{
            "TIMES":3,
            "TIME":1
        },
        "mysql":{
            "HOST":"10.216.141.30",
            "PORT":19002,
            "USERNAME":"devops_master",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    m=Run(config)
    m.run()