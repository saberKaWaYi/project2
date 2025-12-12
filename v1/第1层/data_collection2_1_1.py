import os
import logging
from logging.handlers import RotatingFileHandler
from bson import ObjectId
from connect import Connect_Mongodb,Connect_Mysql
import pandas as pd
from concurrent.futures import ThreadPoolExecutor,as_completed
from ssh import SSH_Server
import time

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

class Run:

    def __init__(self,config1,config2):
        self.config1=config1
        self.config2=config2
        pipeline=[
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
        db_mongo=Connect_Mongodb(self.config1)
        self.data=pd.DataFrame(list(db_mongo.db.cds_ci_att_value_server.aggregate(pipeline))).astype(str).values.tolist()
        self.zd={}

    def run(self):
        with ThreadPoolExecutor(max_workers=25) as executor:
            pool=[]
            for i in self.data:
                pool.append(executor.submit(self.fc,i))
            for task in as_completed(pool):
                task.result()
        for i in self.zd:
            self.zd[i]="|".join(list(set(self.zd[i])))
        db_mysql=Connect_Mysql(self.config2)
        cursor=db_mysql.client.cursor()
        insert_sql="""
        INSERT INTO podx_clux_all_ipv4 (podx_clux, all_ipv4) VALUES (%s, %s) ON DUPLICATE KEY UPDATE all_ipv4 = VALUES (all_ipv4);
        """
        cursor.executemany(insert_sql,[(k,v) for k,v in self.zd.items()])
        db_mysql.client.commit()

    def fc(self,_):
        hostname=_[1];device_ip=_[2];brand=_[3]
        if "POD" not in hostname or "CLU" not in hostname:
            return
        temp=hostname.split("-")
        x,y=None,None
        for i in temp:
            if "POD" in i:
                x=i
            if "CLU" in i:
                y=i
        s=f"{x}-{y}"
        if s not in self.zd:
            self.zd[s]=[]
        client=SSH_Server(hostname,device_ip,brand).client
        if client==None:
            return
        for i in range(5):
            try:
                stdin,stdout,stderr=client.exec_command("esxcfg-vmknic -l |grep IPv4")
                output=stdout.read().decode('utf-8').strip()
                for i in output.split("\n"):
                    temp=i.split()
                    self.zd[s].append(temp[3])
                break
            except Exception as e:
                time.sleep(1)
        else:
            logging.error(f"{hostname:<50s}{device_ip:<25s}{brand:<25s}执行命令失败第{i:02d}。{e}")
        client.close()

if __name__=="__main__":
    config1={
        "connection":{
            "TIMES":1000,
            "TIME":0.1
        },
        "mongodb":{
            "HOST":"10.216.141.46",
            "PORT":27017,
            "USERNAME":"manager",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    config2={
        "connection":{
            "TIMES":1000,
            "TIME":0.1
        },
        "mysql":{
            "HOST":"10.216.141.30",
            "PORT":19002,
            "USERNAME":"devops_master",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    m=Run(config1,config2)
    m.run()