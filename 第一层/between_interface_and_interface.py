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

handler=get_rotating_handler("between_interface_and_interface.log")
logging_between_interface_and_interface=logging.getLogger("between_interaface_and_interface")
logging_between_interface_and_interface.setLevel(logging.INFO)
logging_between_interface_and_interface.addHandler(handler)

from connect import Connect_Mysql
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor,as_completed
import requests
import subprocess
import re

class Run:

    def __init__(self,config):
        self.config=config
        self.conn=Connect_Mysql(self.config);self.lock_sql=threading.Lock()
        self.conn_client=self.conn.client.cursor()
        self.data=self.conn.get_table_data("","select hostname,ip,brand from topu.network")[["hostname","ip","brand"]].values.tolist()
        self.zd=dict(zip([i[0] for i in self.data],[[i[1],i[2]] for i in self.data]))
        self.result=[];self.lock1=threading.Lock()
        self.info=[];self.lock2=threading.Lock()
        self.time=datetime.now()

    def run(self):
        self.create_table()
        self.truncate_table()
        self.run_main()
        self.transform_result()
        self.insert_data()

    def create_table(self):
        sql='''
        create table if not exists topu.between_interface_and_interface (
            lldpLocSysName VARCHAR(100),
            lldpRemSysName VARCHAR(100),
            lldpLocChassisId VARCHAR(100),
            lldpRemChassisId VARCHAR(100),
            lldpLocSysDesc TEXT,
            lldpRemSysDesc TEXT,
            lldpLocPortId VARCHAR(100),
            lldpRemPortId VARCHAR(100),
            lldpLocPortDesc TEXT,
            lldpRemPortDesc TEXT,
            lldpLocip VARCHAR(25),
            lldpRemip VARCHAR(25),
            lldpLocbrand VARCHAR(25),
            lldpRembrand VARCHAR(25)
        );
        '''
        self.conn_client.execute(sql)
        self.conn.client.commit()
        sql='''
        create table if not exists cds_report.collect_lldp_from_network ( 
            hostname VARCHAR(100),
            ip VARCHAR(100),
            brand VARCHAR(100),
            info TEXT,
            time DATE,
            status TINYINT(1)
        );
        '''
        self.conn_client.execute(sql)
        self.conn.client.commit()

    def truncate_table(self):
        sql='''
        truncate table topu.between_interface_and_interface;
        '''
        self.conn_client.execute(sql)
        self.conn.client.commit()
        sql='''
        truncate table cds_report.collect_lldp_from_network;
        '''
        self.conn_client.execute(sql)
        self.conn.client.commit()

    def tool1(self,s):
        lt=[]
        for i in s.split("-"):
            lt.append(i[:2]);lt.append(i[2:])
        return ":".join(lt)
    
    def tool2(self,command):
        process=subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        return process.stdout.strip().replace("\n"," ").split("iso")[1:]
    
    def tool3(self,command):
        process=subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
        info=process.stdout.readline().strip()
        try:
            process.terminate();process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill();process.wait()
        return info

    def use_command(self,hostname,ip,brand,command):
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
            with self.lock2:
                self.info.append((hostname,ip,brand,f"{url_get}没数据。",self.time,0))
            logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{url_get}没数据。")
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
            with self.lock2:
                self.info.append((hostname,ip,brand,f"{url_post}没数据。",self.time,0))
            logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{url_post}没数据。")
            return
        if not response_url_post:
            if "client_name" in config:
                del config["client_name"]
            try:
                response_url_post=requests.post(url_post,config).json()["data"]["cmd_result"]
            except:
                with self.lock2:
                    self.info.append((hostname,ip,brand,f"{url_post}没数据。",self.time,0))
                logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{url_post}没数据。")
                return
            if not response_url_post:
                with self.lock2:
                    self.info.append((hostname,ip,brand,f"{url_post}没数据。",self.time,0))
                logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{url_post}没数据。")
                return
        if brand=="huarong" or brand=="huawei":
            info=response_url_post.split("\n")
            zd={}
            loc_interface=None;neighbor_index=None
            for i in range(len(info)):
                if "has" in info[i] and "neighbor(s):" in info[i]:
                    loc_interface=info[i].split()[0].strip()
                if "Neighbor index" in info[i]:
                    neighbor_index=info[i].split(":")[-1].strip()
                    zd[(loc_interface,neighbor_index)]={}
                if loc_interface==None or neighbor_index==None:
                    continue
                if "System name" in info[i]:
                    j=i
                    if ":" not in info[j]:
                        j+=1
                    zd[(loc_interface,neighbor_index)]["lldpRemSysName"]=info[j][info[j].index(":")+1:].strip()
                if "Chassis ID" in info[i]:
                    j=i
                    if ":" not in info[j]:
                        j+=1
                    zd[(loc_interface,neighbor_index)]["lldpRemChassisId"]=self.tool1(info[j][info[j].index(":")+1:].strip())
                if "System description" in info[i]:
                    zd[(loc_interface,neighbor_index)]["lldpRemSysDesc"]=""
                    j=i
                    if ":" not in info[j]:
                        j+=1
                    zd[(loc_interface,neighbor_index)]["lldpRemSysDesc"]+=info[j][info[j].index(":")+1:].strip()+" "
                    j+=1
                    while True:
                        if ":" in info[j] or info[j]=="":
                            break
                        zd[(loc_interface,neighbor_index)]["lldpRemSysDesc"]+=info[j]+" "
                        j+=1
                    zd[(loc_interface,neighbor_index)]["lldpRemSysDesc"]=zd[(loc_interface,neighbor_index)]["lldpRemSysDesc"].strip()
                if "Port ID subtype" in info[i]:
                    j=i
                    if ":" not in info[j]:
                        j+=1
                    if info[j][info[j].index(":")+1:].strip()=="Interface Name":
                        k=j+1
                        if ":" not in info[k]:
                            k+=1
                        zd[(loc_interface,neighbor_index)]["lldpRemPortId"]=info[k][info[k].index(":")+1:].strip()
                    elif info[j][info[j].index(":")+1:].strip()=="MAC Address":
                        k=j+1
                        if ":" not in info[k]:
                            k+=1
                        zd[(loc_interface,neighbor_index)]["lldpRemPortId"]=self.tool1(info[k][info[k].index(":")+1:].strip())
                if "Port description" in info[i]:
                    j=i
                    if ":" not in info[j]:
                        j+=1
                    zd[(loc_interface,neighbor_index)]["lldpRemPortDesc"]=info[j][info[j].index(":")+1:].strip()
            for i in zd:
                lldpLocSysName=hostname
                lldpRemSysName=zd[i]["lldpRemSysName"] if "lldpRemSysName" in zd[i] and zd[i]["lldpRemSysName"]!="--" else ""
                lldpLocChassisId=""
                lldpRemChassisId=zd[i]["lldpRemChassisId"] if "lldpRemChassisId" in zd[i] and zd[i]["lldpRemChassisId"]!="--" else ""
                lldpLocSysDesc=""
                lldpRemSysDesc=zd[i]["lldpRemSysDesc"] if "lldpRemSysDesc" in zd[i] and zd[i]["lldpRemSysDesc"]!="--" else ""
                lldpLocPortId=i[0]
                lldpRemPortId=zd[i]["lldpRemPortId"] if "lldpRemPortId" in zd[i] and zd[i]["lldpRemPortId"]!="--" else ""
                lldpLocPortDesc=""
                lldpRemPortDesc=zd[i]["lldpRemPortDesc"] if "lldpRemPortDesc" in zd[i] and zd[i]["lldpRemPortDesc"]!="--" else ""
                with self.lock1:
                    self.result.append((lldpLocSysName,lldpRemSysName,lldpLocChassisId,lldpRemChassisId,lldpLocSysDesc,lldpRemSysDesc,lldpLocPortId,lldpRemPortId,lldpLocPortDesc,lldpRemPortDesc,ip,self.zd.get(lldpRemSysName,["",""])[0],brand,self.zd.get(lldpRemSysName,["",""])[1]))
        elif brand=="fenghuo":
            info=response_url_post.split("\n")[3:]
            with self.lock_sql:
                interfaces=set(self.conn.get_table_data("",f"select name from topu.between_network_and_interface where hostname='{hostname}'")["name"].values.tolist())
            for line in info:
                if hostname in line:
                    break
                line=line.split()
                lldpLocSysName=hostname
                lldpRemSysName=line[-1]
                lldpLocChassisId=""
                lldpRemChassisId=""
                lldpLocSysDesc=""
                lldpRemSysDesc=""
                lldpLocPortId=line[0]
                if "mgt-eth" in lldpLocPortId:
                    lldpLocPortId=" ".join([lldpLocPortId.split("mgt-eth")[0],"mgt-eth",lldpLocPortId.split("mgt-eth")[-1]]).strip()
                elif "ge" in lldpLocPortId:
                    x=lldpLocPortId.replace("ge","gigaethernet")
                    x=x.split("gigaethernet")
                    x=x[0]+"gigaethernet"+" "+x[-1]
                    y=lldpLocPortId.replace("ge","GigaEthernet")
                    if x in interfaces:
                        lldpLocPortId=x
                    if y in interfaces:
                        lldpLocPortId=y
                lldpRemPortId=line[4]
                if "gigaethernet" in lldpRemPortId or "mgt-eth" in lldpRemPortId:
                    lldpRemPortId+=" "+line[5]
                lldpLocPortDesc=""
                lldpRemPortDesc=""
                with self.lock1:
                    self.result.append((lldpLocSysName,lldpRemSysName,lldpLocChassisId,lldpRemChassisId,lldpLocSysDesc,lldpRemSysDesc,lldpLocPortId,lldpRemPortId,lldpLocPortDesc,lldpRemPortDesc,ip,self.zd.get(lldpRemSysName,["",""])[0],brand,self.zd.get(lldpRemSysName,["",""])[1]))
        elif brand=="cisco":
            if command=="show cdp neighbors":
                if "CDP is not enabled" in response_url_post:
                    return False
                info=response_url_post.split("\n")
                for i in range(len(info)):
                    line=info[i]
                    if hostname in line:
                        break
                    if len(line.split())!=1:
                        continue
                    if "-" not in line:
                        continue
                    lldpLocSysName=hostname
                    lldpRemSysName=line if "(" not in line else line[:line.index("(")]
                    lldpLocChassisId=""
                    lldpRemChassisId=""
                    lldpLocSysDesc=""
                    lldpRemSysDesc=""
                    line=info[i+1]
                    if "Gig" in line:
                        line=line.split()
                        lldpLocPortId="Gig"+line[1]
                        lldpRemPortId="Gig"+line[-1]
                    elif "Eth" in line:
                        line=line.split()
                        lldpLocPortId=line[0]
                        lldpRemPortId=line[-1]
                    lldpLocPortDesc=""
                    lldpRemPortDesc=""
                    with self.lock1:
                        self.result.append((lldpLocSysName,lldpRemSysName,lldpLocChassisId,lldpRemChassisId,lldpLocSysDesc,lldpRemSysDesc,lldpLocPortId,lldpRemPortId,lldpLocPortDesc,lldpRemPortDesc,ip,self.zd.get(lldpRemSysName,["",""])[0],brand,self.zd.get(lldpRemSysName,["",""])[1]))
            elif command=="show lldp neighbors":
                info=response_url_post.split("\n")[7:]
                for line in info:
                    line=line.split()
                    if len(line)!=5:
                        break
                    lldpLocSysName=hostname
                    lldpRemSysName=line[0].strip(".").strip("-")
                    lldpLocChassisId=""
                    lldpRemChassisId=""
                    lldpLocSysDesc=""
                    lldpRemSysDesc=""
                    lldpLocPortId=line[1] if "[" not in line[1] else line[1][:line[1].index("[")]
                    lldpRemPortId=line[-1]
                    lldpLocPortDesc=""
                    lldpRemPortDesc=""
                    with self.lock1:
                        self.result.append((lldpLocSysName,lldpRemSysName,lldpLocChassisId,lldpRemChassisId,lldpLocSysDesc,lldpRemSysDesc,lldpLocPortId,lldpRemPortId,lldpLocPortDesc,lldpRemPortDesc,ip,self.zd.get(lldpRemSysName,["",""])[0],brand,self.zd.get(lldpRemSysName,["",""])[1]))
        with self.lock2:
            self.info.append((hostname,ip,brand,"",self.time,1))
        if brand=="cisco" and command=="show cdp neighbors":
            return True
     
    def use_snmpwalk(self,hostname,ip,brand):
        if brand=="nokia":
            try:
                with self.lock_sql:
                    interfaces=set(self.conn.get_table_data("",f"select name from topu.between_network_and_interface where hostname='{hostname}'")["name"].values.tolist())
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.3.6.1.4.1.6527.3.1.2.59.3.1.1.4"
                temp=self.tool2(command)
                zd1={}
                for line in temp:
                    line=line.strip()
                    key=line.split(".")[-2]
                    value=line[line.index("\"")+1:][:-1].split(",")[0].strip()
                    zd1[key]=value
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.3.6.1.4.1.6527.3.1.2.59.4.1.1.8"
                temp=self.tool2(command)
                zd2={}
                for line in temp:
                    line=line.strip()
                    key=line.split(".")[-3]
                    value=line[line.index("\"")+1:][:-1].split(",")[0].strip()
                    zd2[key]=value
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.3.6.1.4.1.6527.3.1.2.59.4.1.1.9"
                temp=self.tool2(command)
                zd3={}
                for line in temp:
                    line=line.strip()
                    key=line.split(".")[-3]
                    value=line[line.index("\"")+1:][:-1].split(",")[0].strip()
                    zd3[key]=value
                for i in zd3:
                    lldpLocSysName=hostname
                    lldpRemSysName=zd3[i]
                    lldpLocChassisId=""
                    lldpRemChassisId=""
                    lldpLocSysDesc=""
                    lldpRemSysDesc=""
                    lldpLocPortId=zd1.get(i,None)
                    if not lldpLocPortId or lldpLocPortId not in interfaces:
                        continue
                    lldpRemPortId=zd2.get(i,None)
                    if not lldpRemPortId:
                        continue
                    lldpLocPortDesc=""
                    lldpRemPortDesc=""
                    with self.lock1:
                        self.result.append((lldpLocSysName,lldpRemSysName,lldpLocChassisId,lldpRemChassisId,lldpLocSysDesc,lldpRemSysDesc,lldpLocPortId,lldpRemPortId,lldpLocPortDesc,lldpRemPortDesc,ip,self.zd.get(lldpRemSysName,["",""])[0],brand,self.zd.get(lldpRemSysName,["",""])[1]))
                with self.lock2:
                    self.info.append((hostname,ip,brand,"",self.time,1))
            except Exception as e:
                with self.lock2:
                    self.info.append((hostname,ip,brand,str(e),self.time,0))
                logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{str(e)}")
        elif brand=="huawei":
            command=f"snmpwalk -v 2c -c QAZXSWedc {ip} iso.3.6.1.2.1.1.1.0"
            s="".join(self.tool2(command));s=s[s.index(":")+1:].strip().strip("\"").strip() if ":" in s else ""
            if not s:
                logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}没有型号。")
                return False
            if "-" in s.split()[0]:
                kind=s.split()[0]
            elif "-" in s.split()[-1]:
                kind=s.split()[-1]
            else:
                kind=s.split("Huawei")[0]
                logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{kind:<50s}型号未知。")
                return False
            version=re.findall(r'\((.*?)\)',s)[1]
            command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802"
            info=self.tool3(command)
            if "No Such" in info:
                logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{kind:<50s}{version:<50s}不支持1.0.8802。")
                return False
            command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.7.1.3"
            info=self.tool3(command)
            if "No Such" in info:
                logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{kind:<50s}{version:<50s}没有本地接口。")
                return False
            lldpLocSysName=hostname
            command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.2"
            info=self.tool2(command)[0]
            lldpLocChassisId=info[info.index(":")+1:].strip().strip("\"").strip() if ":" in info else ""
            lldpLocSysDesc=s
            zd={}
            command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.7.1.3"
            for line in self.tool2(command):
                key=line.split()[0].split(".")[-1]
                if key not in zd:
                    zd[key]={}
                zd[key]["lldpLocPortId"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
            command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.7.1.4"
            for line in self.tool2(command):
                key=line.split()[0].split(".")[-1]
                if key not in zd:
                    zd[key]={}
                zd[key]["lldpLocPortDesc"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
            command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.5"
            info=self.tool3(command)
            if "No Such" in info:
                logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{kind:<50s}{version:<50s}没有对端接口。")
                return False
            command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.9"
            for line in self.tool2(command):
                key=line.split()[0].split(".")[-2]
                if key not in zd:
                    zd[key]={}
                zd[key]["lldpRemSysName"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
            command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.5"
            for line in self.tool2(command):
                key=line.split()[0].split(".")[-2]
                if key not in zd:
                    zd[key]={}
                zd[key]["lldpRemChassisId"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
            command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.10"
            for line in self.tool2(command):
                key=line.split()[0].split(".")[-2]
                if key not in zd:
                    zd[key]={}
                zd[key]["lldpRemSysDesc"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
            command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.7"
            for line in self.tool2(command):
                key=line.split()[0].split(".")[-2]
                if key not in zd:
                    zd[key]={}
                zd[key]["lldpRemPortId"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
            command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.8"
            for line in self.tool2(command):
                key=line.split()[0].split(".")[-2]
                if key not in zd:
                    zd[key]={}
                zd[key]["lldpRemPortDesc"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
            for i in zd:
                with self.lock1:
                    self.result.append((lldpLocSysName,zd[i].get("lldpRemSysName",""),lldpLocChassisId,zd[i].get("lldpRemChassisId",""),lldpLocSysDesc,zd[i].get("lldpRemSysDesc",""),zd[i].get("lldpLocPortId",""),zd[i].get("lldpRemPortId",""),zd[i].get("lldpLocPortDesc",""),zd[i].get("lldpRemPortDesc",""),ip,self.zd.get(zd[i].get("lldpRemSysName",""),["",""])[0],brand,self.zd.get(zd[i].get("lldpRemSysName",""),["",""])[1]))
            with self.lock2:
                self.info.append((hostname,ip,brand,"",self.time,1))
            return True
        elif brand=="junos" or brand=="h3c":
            try:
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802"
                info=self.tool3(command)
                if "No Such" in info:
                    with self.lock2:
                        self.info.append((hostname,ip,brand,f"{hostname:<50s}不支持1.0.8802。",self.time,0))
                    logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}不支持1.0.8802。")
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.7.1.3"
                info=self.tool3(command)
                if "No Such" in info:
                    with self.lock2:
                        self.info.append((hostname,ip,brand,f"{hostname:<50s}没有本地接口。",self.time,0))
                    logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}没有本地接口。")
                lldpLocSysName=hostname
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.2"
                info=self.tool2(command)[0]
                lldpLocChassisId=info[info.index(":")+1:].strip().strip("\"").strip() if ":" in info else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.3"
                info=self.tool2(command)[0]
                lldpLocSysDesc=info[info.index(":")+1:].strip().strip("\"").strip() if ":" in info else ""
                zd={}
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.7.1.3"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-1]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpLocPortId"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.7.1.4"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-1]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpLocPortDesc"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.5"
                info=self.tool3(command)
                if "No Such" in info:
                    with self.lock2:
                        self.info.append((hostname,ip,brand,f"{hostname:<50s}没有对端接口。",self.time,0))
                    logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}没有对端接口。")
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.9"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-2]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpRemSysName"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.5"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-2]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpRemChassisId"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.10"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-2]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpRemSysDesc"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.7"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-2]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpRemPortId"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.8"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-2]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpRemPortDesc"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                for i in zd:
                    with self.lock1:
                        self.result.append((lldpLocSysName,zd[i].get("lldpRemSysName",""),lldpLocChassisId,zd[i].get("lldpRemChassisId",""),lldpLocSysDesc,zd[i].get("lldpRemSysDesc",""),zd[i].get("lldpLocPortId",""),zd[i].get("lldpRemPortId",""),zd[i].get("lldpLocPortDesc",""),zd[i].get("lldpRemPortDesc",""),ip,self.zd.get(zd[i].get("lldpRemSysName",""),["",""])[0],brand,self.zd.get(zd[i].get("lldpRemSysName",""),["",""])[1]))
                with self.lock2:
                    self.info.append((hostname,ip,brand,"",self.time,1))
            except Exception as e:
                with self.lock2:
                    self.info.append((hostname,ip,brand,str(e),self.time,0))
                logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{str(e)}。")
        elif brand=="cisco":
            try:
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802"
                info=self.tool3(command)
                if "No Such" in info:
                    logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}不支持1.0.8802。")
                    return False
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.7.1.3"
                info=self.tool3(command)
                if "No Such" in info:
                    logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}没有本地接口。")
                    return False
                lldpLocSysName=hostname
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.2"
                info=self.tool2(command)[0]
                lldpLocChassisId=info[info.index(":")+1:].strip().strip("\"").strip() if ":" in info else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.3"
                info=self.tool2(command)[0]
                lldpLocSysDesc=info[info.index(":")+1:].strip().strip("\"").strip() if ":" in info else ""
                zd={}
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.7.1.3"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-1]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpLocPortId"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.3.7.1.4"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-1]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpLocPortDesc"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.5"
                info=self.tool3(command)
                if "No Such" in info:
                    logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}没有对端接口。")
                    return False
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.9"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-2]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpRemSysName"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.5"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-2]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpRemChassisId"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.10"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-2]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpRemSysDesc"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.7"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-2]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpRemPortId"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                command=f"snmpwalk -v 2c -c QAZXSWedc {ip} 1.0.8802.1.1.2.1.4.1.1.8"
                for line in self.tool2(command):
                    key=line.split()[0].split(".")[-2]
                    if key not in zd:
                        zd[key]={}
                    zd[key]["lldpRemPortDesc"]=line[line.index(":")+1:].strip().strip("\"").strip() if ":" in line else ""
                for i in zd:
                    with self.lock1:
                        self.result.append((lldpLocSysName,zd[i].get("lldpRemSysName",""),lldpLocChassisId,zd[i].get("lldpRemChassisId",""),lldpLocSysDesc,zd[i].get("lldpRemSysDesc",""),zd[i].get("lldpLocPortId",""),zd[i].get("lldpRemPortId",""),zd[i].get("lldpLocPortDesc",""),zd[i].get("lldpRemPortDesc",""),ip,self.zd.get(zd[i].get("lldpRemSysName",""),["",""])[0],brand,self.zd.get(zd[i].get("lldpRemSysName",""),["",""])[1]))
                with self.lock2:
                    self.info.append((hostname,ip,brand,"",self.time,1))
                return True
            except Exception as e:
                logging_between_interface_and_interface.error(f"{hostname:<50s}{ip:<25s}{brand:<25s}{str(e)}。")
                return False

    def fc(self,hostname,ip,brand):
        try:
            if "." not in ip:
                with self.lock2:
                    self.info.append((hostname,ip,brand,"ip有错误",self.time,0))
                logging_between_interface_and_interface.error(f"{hostname},{ip},{brand}。ip有错误。")
                return
            if brand=="huarong":
                self.use_command(hostname,ip,brand,"display lldp neighbor")
            elif brand=="fenghuo":
                self.use_command(hostname,ip,brand,"show lldp remote")
            elif brand=="nokia":
                self.use_snmpwalk(hostname,ip,brand)
            elif brand=="huawei":
                if not self.use_snmpwalk(hostname,ip,brand):
                    self.use_command(hostname,ip,brand,"display lldp neighbor")
            elif brand=="junos" or brand=="h3c":
                self.use_snmpwalk(hostname,ip,brand)
            elif brand=="cisco":
                if not self.use_snmpwalk(hostname,ip,brand):
                    if not self.use_command(hostname,ip,brand,"show cdp neighbors"):
                        self.use_command(hostname,ip,brand,"show lldp neighbors")
            else:
                with self.lock2:
                    self.info.append((hostname,ip,brand,"未知品牌",self.time,0))
                logging_between_interface_and_interface.error(f"{hostname},{ip},{brand}。未知品牌。")
                return
        except Exception as e:
            with self.lock2:
                self.info.append((hostname,ip,brand,str(e),self.time,0))
            logging_between_interface_and_interface.error(f"{hostname},{ip},{brand}。{str(e)}")
            return

    def run_main(self):
        with ThreadPoolExecutor(max_workers=50) as executor:
            pool=[]
            for i in self.data:
                pool.append(executor.submit(self.fc,i[0],i[1],i[2]))
            for task in as_completed(pool):
                task.result()

    def tool4(self,s):
        for i in ["TenGigE","Ethernet","GigabitEthernet","Bundle-Ether"]:
            if i in s:
                return s
        if "Te" in s:
            s=s.replace("Te","TenGigE")
            return s
        if "Eth" in s:
            s=s.replace("Eth","Ethernet")
            return s
        if "Gig" in s:
            s=s.replace("Gig","GigabitEthernet")
            return s
        if "Gi" in s:
            s=s.replace("Gi","GigabitEthernet")
            return s
        if "BE" in s:
            s=s.replace("BE","Bundle-Ether")
            return s

    def transform_result(self):
        temp_result=[]
        for i in self.result:
            temp=list(i)
            temp[0]=temp[0][:100] if temp[0]!=None else None;temp[1]=temp[1][:100] if temp[1]!=None else None;temp[2]=temp[2][:100] if temp[2]!=None else None;temp[3]=temp[3][:100] if temp[3]!=None else None
            temp[6]=temp[6][:100] if temp[6]!=None else None;temp[7]=temp[7][:100] if temp[7]!=None else None
            temp[-4]=temp[-4][:25] if temp[-4]!=None else None;temp[-3]=temp[-3][:25] if temp[-3]!=None else None;temp[-2]=temp[-2][:25] if temp[-2]!=None else None;temp[-1]=temp[-1][:25] if temp[-1]!=None else None
            temp[6]=self.tool4(temp[6]) if temp[-2]=="cisco" else temp[6]
            temp[7]=self.tool4(temp[7]) if temp[-1]=="cisco" else temp[7]
            temp_result.append(tuple(temp))
        self.result=temp_result

    def insert_data(self):
        self.conn=Connect_Mysql(self.config)
        self.conn_client=self.conn.client.cursor()
        sql='''
        insert into topu.between_interface_and_interface (lldpLocSysName,lldpRemSysName,lldpLocChassisId,lldpRemChassisId,lldpLocSysDesc,lldpRemSysDesc,lldpLocPortId,lldpRemPortId,lldpLocPortDesc,lldpRemPortDesc,lldpLocip,lldpRemip,lldpLocbrand,lldpRembrand) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        self.conn_client.executemany(sql,self.result)
        self.conn.client.commit()
        sql='''
        insert into cds_report.collect_lldp_from_network (hostname,ip,brand,info,time,status) values (%s,%s,%s,%s,%s,%s)
        '''
        self.conn_client.executemany(sql,self.info)
        self.conn.client.commit()

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