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

handler=get_rotating_handler("between_interface_and_nic_preparation2.log")
logging_between_interface_and_nic_preparation2=logging.getLogger("between_interface_and_nic_preparation2")
logging_between_interface_and_nic_preparation2.setLevel(logging.INFO)
logging_between_interface_and_nic_preparation2.addHandler(handler)

from connect import Connect_Mysql
from datetime import datetime
import threading
import time
from concurrent.futures import ThreadPoolExecutor,as_completed
from ssh import SSH_Server
import requests

class Run:

    def __init__(self,config):
        self.config=config
        self.db_mysql=Connect_Mysql(config)
        self.db_mysql_client=self.db_mysql.client.cursor()
        data1=self.db_mysql.get_table_data("","select hostname,ip,brand from topu.network")[["hostname","ip","brand"]].values.tolist()
        data2=self.db_mysql.get_table_data("","select hostname,in_band_ip,brand2 from topu.server")[["hostname","in_band_ip","brand2"]].values.tolist()
        data3=self.db_mysql.get_table_data("","select * from topu.temp")[["name","ipv4_list"]].values.tolist()
        self.zd1={};self.zd1["else"]=[]
        for i in data1:
            temp=i[0].split("-")
            for j in temp:
                if "POD" in j:
                    if j not in self.zd1:
                        self.zd1[j]=[]
                    self.zd1[j].append([i[0],i[1],i[2]])
                    break
            else:
                self.zd1["else"].append([i[0],i[1],i[2]])
        self.zd2={};self.zd2["else"]=[]
        for i in data2:
            temp=i[0].split("-")
            x,y=None,None
            for j in temp:
                if "POD" in j:
                    x=j
                if "CLU" in j:
                    y=j
            if x and y:
                if f"{x}-{y}" not in self.zd2:
                    self.zd2[f"{x}-{y}"]=[]
                self.zd2[f"{x}-{y}"].append([i[0],i[1],i[2]])
            else:
                self.zd2["else"].append([i[0],i[1],i[2]])
        self.zd3={}
        for i in data3:
            if not i[1]:
                continue
            self.zd3[i[0]]=i[1].split("|")
        self.zd4={}
        for i in self.zd3.keys():
            temp=i.split("-")
            for j in temp:
                if "POD" in j:
                    if j not in self.zd4:
                        self.zd4[j]=[]
                    self.zd4[j].append(i)
                    break
        self.time=datetime.now()
        self.signal1=None
        self.errors=[];self.lock1=threading.Lock()
        self.signal2=None;self.lock2=threading.Lock()
        self.signal3=None;self.lock3=threading.Lock()
        self.result={};self.lock4=threading.Lock()

    def alter(self):
        sql='''
        SELECT column_name AS temp FROM information_schema.columns WHERE table_schema = 'topu' AND table_name = 'interface'
        '''
        columns=self.db_mysql.get_table_data("",sql)["temp"].values.tolist()
        if "mac_address" in columns:
            return
        sql="ALTER TABLE topu.interface ADD COLUMN mac_address MEDIUMTEXT;"
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()

    def tool1(self,s,split_s):
        s=s.split(split_s)
        result=[]
        for i in s:
            result.append(i[:2])
            result.append(i[2:])
        return ":".join(result)
    
    def tool2(self,s,brand):
        if brand=="huawei" or brand=="huarong":
            if s[0]=="#":
                s=s[1:]
            if "GE" in s and s.index("GE")==0:
                s=s.replace("GE","GigabitEthernet")
            if "XGE" in s and s.index("XGE")==0:
                s=s.replace("XGE","XGigabitEthernet")
        elif brand=="cisco":
            if "Eth" in s:
                s=s.replace("Eth","Ethernet")
        elif brand=="h3c":
            if "GE" in s and s.index("GE")==0:
                s=s.replace("GE","GigabitEthernet")
            if "XGE" in s and s.index("XGE")==0:
                s=s.replace("XGE","XGigabitEthernet")
            if "BAGG" in s and s.index("BAGG")==0:
                s=s.replace("BAGG","Bridge-Aggregation")
        return s

    def fc(self,hostname,ip,brand,command):
        url_post='http://10.213.136.111:40061/network_app/distribute_config/exec_cmd/'
        config={
            "device_hostname":hostname,
            "operator":"LCL",
            "is_edit":False,
            "cmd":""
        }
        config["cmd"]=command
        url_get=f"http://10.216.142.10:40061/network_app/conf/device/data_list/?device_hostname={hostname}"
        response_url_get=requests.get(url_get).json()
        try:
            client_names=response_url_get["data"][0]["net_data"]
        except:
            with self.lock1:
                self.errors.append((hostname,ip,brand,f"{url_get}没数据。",self.time,"between_interface_and_nic_preparation2.py"))
            logging_between_interface_and_nic_preparation2.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{url_get}没数据。")
            return
        if client_names:
            temp=[]
            for client_name in client_names.values():
                if client_name["operator"]!="LCL":
                    continue
                temp.append((client_name["client_name"],client_name["created_at"]))
            temp.sort(key=lambda x:x[1])
            if temp:
                config["client_name"]=temp[-1][0]
        try:
            response_url_post=requests.post(url_post,config).json()["data"]["cmd_result"]
        except:
            with self.lock1:
                self.errors.append((hostname,ip,brand,f"{url_post}没数据。",self.time,"between_interface_and_nic_preparation2.py"))
            logging_between_interface_and_nic_preparation2.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{url_post}没数据。")
            return
        if not response_url_post:
            if "client_name" in config:
                del config["client_name"]
            try:
                response_url_post=requests.post(url_post,config).json()["data"]["cmd_result"]
            except:
                with self.lock1:
                    self.errors.append((hostname,ip,brand,f"{url_post}没数据。",self.time,"between_interface_and_nic_preparation2.py"))
                logging_between_interface_and_nic_preparation2.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{url_post}没数据。")
                return
            if not response_url_post:
                with self.lock1:
                    self.errors.append((hostname,ip,brand,f"{url_post}没数据。",self.time,"between_interface_and_nic_preparation2.py"))
                logging_between_interface_and_nic_preparation2.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{url_post}没数据。")
                return
        if brand=="huawei" or brand=="huarong":
            info=response_url_post.split("\n")
            for line in info:
                line=line.split()
                if not line or line[0].count("-")!=2 or line[3]!="dynamic":
                    continue
                with self.lock4:
                    if (hostname,self.tool2(line[2],brand)) not in self.result:
                        self.result[(hostname,self.tool2(line[2],brand))]=[]
                    self.result[(hostname,self.tool2(line[2],brand))].append(self.tool1(line[0],"-"))
        elif brand=="cisco":
            info=response_url_post.split("\n")
            for line in info:
                line=line.split()
                if not line or line[0]!="*":
                    continue
                with self.lock4:
                    if (hostname,self.tool2(line[-1],brand)) not in self.result:
                        self.result[(hostname,self.tool2(line[-1],brand))]=[]
                    self.result[(hostname,self.tool2(line[-1],brand))].append(self.tool1(line[2],"."))
        elif brand=="junos":
            info=response_url_post.split("\n")
            for line in info:
                line=line.split()
                if len(line)<2 or line[1].count(":")!=5:
                    continue
                with self.lock4:
                    if (hostname,line[3]) not in self.result:
                        self.result[(hostname,line[3])]=[]
                    self.result[(hostname,line[3])].append(line[1])
        elif brand=="h3c":
            info=response_url_post.split("\n")
            for line in info:
                line=line.split()
                if not line or line[0].count("-")!=2:
                    continue
                with self.lock4:
                    if (hostname,self.tool2(line[2],brand)) not in self.result:
                        self.result[(hostname,self.tool2(line[2],brand))]=[]
                    self.result[(hostname,self.tool2(line[2],brand))].append(self.tool1(line[0],"-"))

    def main1(self,hostname,ip,brand):
        if "." not in ip:
            with self.lock1:
                self.errors.append((hostname,ip,brand,"ip有错误",self.time,"between_interface_and_nic_preparation2.py"))
            logging_between_interface_and_nic_preparation2.error(f"{hostname},{ip},{brand}。ip有错误。")
            return
        if hostname.lower() in ["none","null","nan","","-","--","---"]:
            with self.lock1:
                self.errors.append((hostname,ip,brand,"hostname有错误",self.time,"between_interface_and_nic_preparation2.py"))
            logging_between_interface_and_nic_preparation2.error(f"{hostname},{ip},{brand}。hostname有错误。")
            return
        if brand not in ["huawei","huarong","cisco","junos","fenghuo","nokia","h3c"]:
            with self.lock1:
                self.errors.append((hostname,ip,brand,"未知品牌",self.time,"between_interface_and_nic_preparation2.py"))
            logging_between_interface_and_nic_preparation2.error(f"{hostname},{ip},{brand}。未知品牌。")
            return
        if brand=="huawei" or brand=="huarong":
            self.fc(hostname,ip,brand,"display mac-address")
        elif brand=="cisco":
            self.fc(hostname,ip,brand,"show mac address-table | in Eth")
        elif brand=="junos":
            self.fc(hostname,ip,brand,"show ethernet-switching table")
        elif brand=="h3c":
            self.fc(hostname,ip,brand,"dis mac-address mac-move")
        elif brand=="fenghuo":
            pass#有待改进
        elif brand=="nokia":
            pass#有待改进

    def run1_0(self,key):
        with ThreadPoolExecutor(max_workers=25) as executor:
            pool=[]
            for i in self.zd1[key]:
                pool.append(executor.submit(self.main1,i[0],i[1],i[2]))
            for task in as_completed(pool):
                task.result()

    def run1(self,key):
        while True:
            if time.time()-self.signal1>=30*60:
                with self.lock1:
                    self.errors.append(("","","",f"{key}采集超时",self.time,"between_interface_and_nic_preparation2.py"))
                logging_between_interface_and_nic_preparation2.error(f"{key}采集超时。")
                break
            with self.lock2:
                nums=sum(self.signal2)
            if nums==len(self.zd4[key]):
                self.run1_0(key)
                break
            time.sleep(1)
        with self.lock3:
            self.signal3=1
        logging_between_interface_and_nic_preparation2.info(f"{key}采集完成。")

    def main2(self,name,iter_):
        if not self.zd3[name]:
            with self.lock2:
                self.signal2[iter_]=1
            return
        lt=[]
        for machine in self.zd2[name]:
            hostname=machine[0];ip=machine[1];brand=machine[2]
            client=SSH_Server(hostname,ip,brand).client
            if client==None:
                continue
            lt.append(client)
        if not lt:
            with self.lock2:
                self.signal2[iter_]=1
            return
        flag=False
        while True:
            if time.time()-self.signal1>=30*60:
                with self.lock1:
                    self.errors.append(("","","",f"{name}预热超时",self.time,"between_interface_and_nic_preparation2.py"))
                logging_between_interface_and_nic_preparation2.error(f"{name}预热超时。")
                break
            with self.lock3:
                if self.signal3:
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
                    client.exec_command(command)
                except Exception as e:
                    logging_between_interface_and_nic_preparation2.error(e)
            if flag:
                continue
            with self.lock2:
                self.signal2[iter_]=1
            flag=True
        for client in lt:
            client.close()

    def run2(self,key):
        self.signal2=[0]*len(self.zd4[key])
        with ThreadPoolExecutor(max_workers=len(self.zd4[key])) as executor:
            pool=[]
            for i in range(len(self.zd4[key])):
                pool.append(executor.submit(self.main2,self.zd4[key][i],i))
            for task in as_completed(pool):
                task.result()
        logging_between_interface_and_nic_preparation2.info(f"{key}预热完成。")

    def main(self):
        for i in self.zd1:
            if i not in self.zd4 and i!="else":
                continue
            if i!="else":
                self.signal1=time.time()
                self.signal2=[]
                self.signal3=0
                t1=threading.Thread(target=self.run1,args=(i,))
                t2=threading.Thread(target=self.run2,args=(i,))
                t1.start()
                t2.start()
                t1.join()
                t2.join()
            else:
                self.run1("else")

    def run(self):
        self.alter()
        self.main()

if __name__=="__main__":
    config={
        "connection":{
            "TIMES":3,
            "TIME":0.1
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