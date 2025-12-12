import os
import logging
from logging.handlers import RotatingFileHandler
from connect import Connect_Mongodb,Connect_Mysql
from bson import ObjectId
import pandas as pd
import threading
import time
from concurrent.futures import ThreadPoolExecutor,as_completed
import requests
from ssh import SSH_Server

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
        data1=pd.DataFrame(list(db_mongo.db.cds_ci_att_value_network.aggregate(pipeline))).astype(str).values.tolist()
        data2=pd.DataFrame(list(db_mongo.db.cds_ci_att_value_server.aggregate(pipeline))).astype(str).values.tolist()
        self.zd1={};self.zd1["else"]=[]
        for i in data1:
            temp=i[2].split("-")
            for j in temp:
                if "POD" in j:
                    if j not in self.zd1:
                        self.zd1[j]=[]
                    self.zd1[j].append([i[1],i[2],i[3]])
                    break
            else:
                self.zd1["else"].append([i[1],i[2],i[3]])
        self.zd2={};self.zd2["else"]=[]
        for i in data2:
            temp=i[1].split("-")
            x,y=None,None
            for j in temp:
                if "POD" in j:
                    x=j
                if "CLU" in j:
                    y=j
            if x and y:
                if f"{x}-{y}" not in self.zd2:
                    self.zd2[f"{x}-{y}"]=[]
                self.zd2[f"{x}-{y}"].append([i[1],i[2],i[3]])
            else:
                self.zd2["else"].append([i[1],i[2],i[3]])
        self.zd3={}
        db_mysql=Connect_Mysql(self.config2)
        data3=db_mysql.get_table_data("podx_clux_all_ipv4","select * from podx_clux_all_ipv4").values.tolist()
        for i in data3:
            if i[1]=="":
                continue
            self.zd3[i[0]]=[]
            for j in i[1].split("|"):
                self.zd3[i[0]].append(j)
        self.zd4={}
        for i in self.zd3.keys():
            temp=i.split("-")
            for j in temp:
                if "POD" in j:
                    if j not in self.zd4:
                        self.zd4[j]=[]
                    self.zd4[j].append(i)
                    break
        self.zd={}
        self.signal1=None
        self.signal2=None
        self.time_signal=None

    def init(self):
        sql="ALTER TABLE interface ADD COLUMN mac_address MEDIUMTEXT;"
        db_mysql=Connect_Mysql(self.config2)
        cursor=db_mysql.client.cursor()
        cursor.execute(sql)
        db_mysql.client.commit()

    def run(self):
        for i in self.zd1:
            if self.zd4.get(i,None)==None and i!="else":
                continue
            if i!="else":
                self.signal1=0
                self.signal2=[]
                self.time_signal=time.time()
                t1=threading.Thread(target=self.run1,args=(i,))
                t2=threading.Thread(target=self.run2,args=(i,))
                t1.start()
                t2.start()
                t1.join()
                t2.join()
            else:
                self.main1("else")
        for i in self.zd.keys():
            self.zd[i]="|".join(list(set(self.zd[i])))
        db_mysql=Connect_Mysql(self.config2)
        data=db_mysql.get_table_data("interface","select hostname,name from interface")
        jh=set()
        for i in data.values.tolist():
            jh.add(i[0]+"|"+i[1])
        for i in self.zd.copy().keys():
            if i not in jh:
                del self.zd[i]
        sql="UPDATE interface SET mac_address = CASE "
        for key in self.zd:
            hostname,name=key.split("|")
            mac_address=self.zd[key]
            sql+=f"WHEN hostname = '{hostname}' AND name = '{name}' THEN '{mac_address}' "
        sql+="END WHERE (hostname, name) IN ("
        for key in self.zd:
            hostname,name=key.split("|")
            sql+=f"('{hostname}', '{name}'), "
        sql=sql.rstrip(", ") + ");"
        cursor=db_mysql.client.cursor()
        cursor.execute(sql)
        db_mysql.client.commit()
    
    def run1(self,key):
        while True:
            if time.time()-self.time_signal>=30*60:
                logging.error("超时警告。")
                return
            if sum(self.signal2)==len(self.zd4[key]):
                time.sleep(30)
                logging.info("*"*100)
                logging.info("开始采集。")
                self.main1(key)
                logging.info("="*100)
                logging.info("采集结束。")
                break
        self.signal1=1

    def run2(self,key):
        self.signal2=[0]*len(self.zd4[key])
        with ThreadPoolExecutor(max_workers=len(self.zd4[key])+1) as executor:
            pool=[]
            for i in range(len(self.zd4[key])):
                pool.append(executor.submit(self.main2,[self.zd4[key][i],i]))
            for task in as_completed(pool):
                task.result()

    def main1(self,key):
        with ThreadPoolExecutor(max_workers=25) as executor:
            pool=[]
            for i in self.zd1[key]:
                pool.append(executor.submit(self.fc1,i))
            for task in as_completed(pool):
                task.result()

    def fc1(self,info):
        hostname,device_ip,brand=info[1],info[0],info[2].lower()
        if hostname.lower()=="none" or hostname.lower()=="null" or hostname.lower()=="nan" or hostname=="" or hostname=="-" or hostname=="--" or hostname=="---" or hostname==None:
            return
        if "CL" not in hostname and "FE" not in hostname:
            return
        if "." not in device_ip:
            return
        if brand=="" or brand=="-" or brand=="--" or brand=="---" or brand=="none" or brand=="null" or brand=="nan" or brand==None:
            return
        try:
            if brand=="huawei" or brand=="huarong":
                self.demo(hostname,brand,"display mac-address")
            elif brand=="cisco":
                self.demo(hostname,brand,"show mac address-table | in Eth")
            elif brand=="junos":
                self.demo(hostname,brand,"show ethernet-switching table")
            elif brand=="h3c":
                self.demo(hostname,brand,"dis mac-address mac-move")
            elif brand=="fenghuo":
                return
            elif brand=="nokia":
                return
        except:
            logging.info(f"{hostname:<50s}{brand:<25s}{device_ip:<25s}mac_address采集失败。")
        
    def demo(self,hostname,brand,command):
        url='http://10.213.136.111:40061/network_app/distribute_config/exec_cmd/'
        data={
            "device_hostname":hostname,
            "operator":"devops",
            "is_edit":False,
            "cmd":command
        }
        response=requests.post(url,json=data)
        temp=response.json()["data"]["cmd_result"].split("\n")
        if brand=="huawei" or brand=="huarong":
            count=0
            for i in temp:
                if "---------------" in i:
                    count+=1
                    continue
                if count==3:
                    break
                if count==2:
                    temp_temp=i.split()
                    if temp_temp==[]:
                        continue
                    id_=hostname+"|"+self.transform_name(brand,temp_temp[2])
                    if id_ not in self.zd:
                        self.zd[id_]=[]
                    self.zd[id_].append(self.transform_format(brand,temp_temp[0]))
        elif brand=="cisco":
            for i in temp:
                if "*" not in i:
                    continue
                temp_temp=i.split()
                id_=hostname+"|"+self.transform_name(brand,temp_temp[-1])
                if id_ not in self.zd:
                    self.zd[id_]=[]
                self.zd[id_].append(self.transform_format(brand,temp_temp[2]))
        elif brand=="junos":
            for i in temp:
                if i.count(":")!=5:
                    continue
                temp_temp=i.split()
                id_=hostname+"|"+self.transform_name(brand,temp_temp[3])
                if id_ not in self.zd:
                    self.zd[id_]=[]
                self.zd[id_].append(self.transform_format(brand,temp_temp[1]))
        elif brand=="h3c":
            for i in temp:
                if i.count("-")<4:
                    continue
                if len(i.split())==1:
                    continue
                if "--" in i:
                    continue
                temp_temp=i.split()
                id_=hostname+"|"+self.transform_name(brand,temp_temp[3])
                if id_ not in self.zd:
                    self.zd[id_]=[]
                self.zd[id_].append(self.transform_format(brand,temp_temp[0]))

    def transform_name(self,brand,name):
        if brand=="huawei" or brand=="huarong" or "h3c":
            if "#"==name[0]:
                name=name[1:]
            if "Eth-Trunk" not in name and "Eth" in name:
                name=name.replace("Eth","Ethernet")
            if "GE" in name and name.index("GE")==0:
                name=name.replace("GE","GigabitEthernet")
            if "XGE" in name:
                name=name.replace("XGE","XGigabitEthernet")
        elif brand=="cisco":
            if "Eth-Trunk" not in name and "Eth" in name:
                name=name.replace("Eth","Ethernet")
        elif brand=="junos":
            if "esi" in name:
                return "esi"
        return name

    def transform_format(self,brand,mac_addresss):
        if brand=="huawei" or brand=="huarong" or brand=="h3c":
            s=[]
            for i in mac_addresss.split("-"):
                s.append(i[:2])
                s.append(i[2:])
        elif brand=="cisco":
            s=[]
            for i in mac_addresss.split("."):
                s.append(i[:2])
                s.append(i[2:])
        elif brand=="junos":
            return mac_addresss
        return ":".join(s)
    
    def main2(self,info):
        name=info[0];iter_=info[1]
        lt=[]
        for machine in self.zd2[name]:
            hostname=machine[0];device_ip=machine[1];brand=machine[2]
            client=SSH_Server(hostname,device_ip,brand).client
            if client==None:
                continue
            else:
                lt.append(client)
        if len(lt)==0:
            self.signal2[iter_]=1
            return
        while True:
            if time.time()-self.time_signal>=30*60:
                logging.error("超时警告。")
                break
            if self.signal1==1:
                break
            for client in lt:
                command=f"""
                #!/bin/sh
                for ip in {' '.join(self.zd3[name])}; do
                    ping -c 1 -W 1 $ip >/dev/null 2>&1 || ping -c 1 -W 1 -S vmotion $ip >/dev/null 2>&1 &
                done
                wait
                """
                try:
                    stdin,stdout,stderr=client.exec_command(command)
                    error=stderr.read().decode('utf-8').strip()
                    logging.info(f"{name}已完成。")
                except Exception as e:
                    logging.error(f"{hostname:<50s}{device_ip:<25s}{brand:<25s}执行命令失败。{e}。{error}。")
            self.signal2[iter_]=1
        for client in lt:
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
    m.init()
    m.run()