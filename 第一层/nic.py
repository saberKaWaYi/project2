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

handler=get_rotating_handler("nic.log")
logging_nic=logging.getLogger("nic")
logging_nic.setLevel(logging.INFO)
logging_nic.addHandler(handler)

from connect import Connect_Mysql,Connect_Mongodb
from bson import ObjectId
import threading
from datetime import datetime
from ssh import SSH_Server
import pandas as pd
from concurrent.futures import ThreadPoolExecutor,as_completed

class Run:

    def __init__(self,config1,config2):
        self.config1=config1
        self.config2=config2
        self.db_mysql=Connect_Mysql(config1)
        self.db_mysql_client=self.db_mysql.client.cursor()
        self.db_mongo=Connect_Mongodb(config2)
        self.db_mongo_client=self.db_mongo.client
        self.pipeline=[
            {
                '$match':{
                    'status':1,
                    'asset_status':{
                        '$in':[
                            ObjectId("5f964e31df0dfd65aaa716ec"),
                            ObjectId("5fcef6de94103c791bc2a471")
                        ]
                    },
                    "device_server_group":ObjectId("5ec8c70a94285cfd9cacee91")
                }
            },
            {
                '$lookup':{
                    'from':'cds_ci_location_detail',
                    'localField':'_id',
                    'foreignField':'device_id',
                    'as':'location'
                }
            },
            {
                '$match':{
                    'location.status':1
                }
            },
            {
                '$project':{
                    "device_ip":1,
                    "brand":1,
                    "hostname":1
                }
            }
        ]
        self.result1=[];self.lock1=threading.Lock()
        self.result2=[];self.lock2=threading.Lock()
        self.result3=[];self.lock3=threading.Lock()
        self.time=datetime.now()
        

    def create_table(self):
        sql='''
        create table if not exists topu.nic (
            hostname VARCHAR(100),
            ip VARCHAR(100),
            brand VARCHAR(100),
            name VARCHAR(25),
            mac_address VARCHAR(50),
            type VARCHAR(25),
            description TEXT,
            primary key (hostname,name)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        create table if not exists topu.between_server_and_nic (
            hostname VARCHAR(100),
            name VARCHAR(25),
            primary key (hostname,name)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        create table if not exists cds_report.collect_lldp_from_server (
            hostname VARCHAR(100),
            ip VARCHAR(100),
            brand VARCHAR(100),
            info TEXT,
            time DATE
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()

    def truncate_table(self):
        sql='''
        truncate table topu.nic;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.between_server_and_nic;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table cds_report.collect_lldp_from_server;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()

    def fc(self,hostname,ip,brand):
        try:
            if "." not in ip:
                with self.lock3:
                    self.result3.append((hostname,ip,brand,"ip有错误",self.time))
                logging_nic.error(f"{hostname},{ip},{brand}。ip有错误。")
                return
            client=SSH_Server(hostname,ip,brand).client
            if client==None:
                with self.lock3:
                    self.result3.append((hostname,ip,brand,"登录不上",self.time))
                logging_nic.error(f"{hostname},{ip},{brand}。登录不上。")
                return
            stdin,stdout,stderr=client.exec_command("esxcfg-vmknic -l | grep IPv4",timeout=60)
            output=stdout.read().decode('utf-8').strip()
            for line in output.split("\n"):
                line=line.split()
                self.result1.append((hostname,ip,brand,line[0],line[6],"虚拟",line[1]))
            stdin,stdout,stderr=client.exec_command("esxcli network nic list",timeout=60)
            output=stdout.read().decode('utf-8').strip()
            for line in output.split("\n"):
                if "nic" not in line:
                    continue
                line=line.split()
                self.result1.append((hostname,ip,brand,line[0],line[7],"物理",line[1]+"|"+line[2]+"|"+line[-1]))
            client.close()
        except Exception as e:
            with self.lock3:
                self.result3.append((hostname,ip,brand,str(e),self.time))
            logging_nic.error(f"{hostname},{ip},{brand}。{str(e)}。")

    def collect(self):
        data=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_server.aggregate(self.pipeline))).astype(str)[["hostname","device_ip","brand"]].values.tolist()
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
        insert into cds_report.collect_lldp_from_server (hostname,ip,brand,info,time) values (%s,%s,%s,%s,%s)
        '''
        self.db_mysql_client.executemany(sql,self.result3)
        self.db_mysql.client.commit()

    def run(self):
        self.create_table()
        self.truncate_table()
        self.collect()
        self.insert_data()

if __name__=="__main__":
    config1={
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
    config2={
        "connection":{
            "TIMES":3,
            "TIME":1
        },
        "mongodb":{
            "HOST":"10.216.141.46",
            "PORT":27017,
            "USERNAME":"manager",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    m=Run(config1,config2)
    m.run()