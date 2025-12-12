import os
import logging
from logging.handlers import RotatingFileHandler
from connect import Connect_Mongodb
from bson import ObjectId
import pandas as pd
from concurrent.futures import ThreadPoolExecutor,as_completed
import requests

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
        db_mongo=Connect_Mongodb(self.config1)
        pipeline1=[
            {
                '$match':{
                    'status':1,
                    'asset_status':{
                        '$in':[
                            ObjectId("5f964e31df0dfd65aaa716ec"),
                            ObjectId("5fcef6de94103c791bc2a471")
                        ]
                    },
                    'brand':{
                        '$in':[
                            'Fenghuo'
                        ]
                    }
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
                    "brand":1,
                    "hostname":1
                }
            }
        ]
        data1=pd.DataFrame(list(db_mongo.db.cds_ci_att_value_network.aggregate(pipeline1))).astype(str)[["hostname","brand"]].values.tolist()
        pipeline2=[
            {
                '$match':{
                    'status':1
                }
            },
            {
                '$project':{
                    "hostname":1,
                    "name":1
                }
            }
        ]
        data2=pd.DataFrame(list(db_mongo.db.cds_ci_att_value_interface.aggregate(pipeline2))).astype(str)[["hostname","name"]].values.tolist()
        self.zd1={}
        for i in data1:
            self.zd1[i[0]]=i[1]
        self.zd2={}
        for i in data2:
            if i[0] not in self.zd1:
                continue
            if i[0] not in self.zd2:
                self.zd2[i[0]]=[]
            self.zd2[i[0]].append(i[1])
    
    def run(self):
        with ThreadPoolExecutor(max_workers=1) as executor:
            pool=[]
            for i,j in self.zd2.items():
                if "CL" not in i and "FE" not in i:
                    continue
                for k in j:
                    pool.append(executor.submit(self.fc,[i,k,self.zd1[i]]))
            for task in as_completed(pool):
                task.result()

    def fc(self,info):
        hostname,name,brand=info[0],info[1],info[2].lower()
        command=f"show interface {name}"
        data={
            "device_hostname":hostname,
            "operator":"devops",
            "is_edit":False,
            "cmd":command
        }
        url=f"http://10.216.142.10:40061/network_app/conf/device/data_list/?device_hostname={hostname}"
        response=requests.get(url).json()
        if response["data"][0]["net_data"]!={}:
            client_name=list(response["data"][0]["net_data"].values())[0]['client_name']
            data["client_name"]=client_name
        else:
            print(f"{hostname}现在会创建一次。")
        url='http://10.213.136.111:40061/network_app/distribute_config/exec_cmd/'
        response=requests.post(url,json=data)
        temp=response.json()["data"]["cmd_result"]
        for i in temp.split("\n"):
            if "Hardware address is" in i:
                temp=i.split()
                print(hostname,name,temp[-1])
                return
        print(hostname,name)
        print(temp)
        return

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