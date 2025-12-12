import os
import logging
from logging.handlers import RotatingFileHandler
from connect import Connect_Mongodb,Connect_Mysql
from bson import ObjectId
import pandas as pd
from concurrent.futures import ThreadPoolExecutor,as_completed
import subprocess
import time
import re
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
        pipeline=[
            {
                '$match':{
                    'status':1,
                    'asset_status':{
                        '$in':[
                            ObjectId("5f964e31df0dfd65aaa716ec"),
                            ObjectId("5fcef6de94103c791bc2a471")
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
                    "device_ip":1,
                    "brand":1,
                    "hostname":1
                }
            }
        ]
        self.data=pd.DataFrame(list(db_mongo.db.cds_ci_att_value_network.aggregate(pipeline))).astype(str).values.tolist()
        self.result=[]

    def run(self):
        with ThreadPoolExecutor(max_workers=50) as executor:
            pool=[]
            for i in self.data:
                pool.append(executor.submit(self.fc,[i[1],i[2],i[3]]))
            for task in as_completed(pool):
                task.result()
        #######################################################################
        try:
            zd=dict(zip([i[0] for i in self.result],[i[2] for i in self.result]))
            for i in self.result:
                i[10]=i[10].split(".")[0]
                if zd.get(i[0],None)=="cisco":
                    i[5]=self.demo4(i[5],"cisco")
                if zd.get(i[0],None)=="fenghuo":
                    i[5]=self.demo4(i[5],"fenghuo",i[0],0)
                if zd.get(i[10],None)=="cisco":
                    i[8]=self.demo4(i[8],"cisco")
                if zd.get(i[10],None)=="fenghuo":
                    i[8]=self.demo4(i[8],"fenghuo",i[0],1)
            columns=['hostname','device_ip','brand','lldpLocChassisId','lldpLocSysDesc','lldpLocPortId','lldpLocPortDesc','lldpRemChassisId','lldpRemPortId','lldpRemPortDesc','lldpRemSysName','lldpRemSysDesc']
            values_placeholders=', '.join(['%s']*len(columns));columns_str=', '.join(columns)
            update_clause=', '.join([f"{column} = VALUES({column})" for column in columns if column not in ["hostname","lldpLocPortId"]])
            sql=f'''
                INSERT INTO interface_to_interface ({columns_str}) VALUES ({values_placeholders}) ON DUPLICATE KEY UPDATE {update_clause};
            '''
            db_mysql=Connect_Mysql(self.config2)
            cursor=db_mysql.client.cursor()
            values_list=[tuple(row) for row in self.result]
            cursor.executemany(sql,values_list)
            db_mysql.client.commit()
        except Exception as e:
            logging.error(e)

    def fc(self,info):
        try:
            hostname,device_ip,brand=info[1].strip(),info[0].strip(),info[2].lower().strip()
            if hostname.lower()=="none" or hostname.lower()=="null" or hostname.lower()=="nan" or hostname=="" or hostname=="-" or hostname=="--" or hostname=="---" or hostname==None:
                return
            if "." not in device_ip:
                return
            if brand=="" or brand=="-" or brand=="--" or brand=="---" or brand=="none" or brand=="null" or brand=="nan" or brand==None:
                return
            #######################################################################
            command=f"ping -c 4 {device_ip}";process=self.demo1(command,1)
            if process.returncode!=0:
                logging.error(f"{device_ip:<25s}{hostname:<25s}{brand:<25s}连接失败。")
                return
            #######################################################################
            if brand in ["cisco","junos","h3c"]:
                command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802";process=self.demo1(command,2)
                first_line=process.stdout.readline().strip()
                try:
                    process.terminate();process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill();process.wait()
                if "No Such" in first_line:
                    if brand=="junos" or brand=="h3c":
                        logging.info(f"{device_ip:<25s}{hostname:<50s}{brand:<25s}不支持1.0.8802。同时也是无法解决。")
                    else:
                        try:
                            try:
                                self.demo3(hostname,device_ip,brand,"show cdp neighbors")
                            except:
                                self.demo3(hostname,device_ip,brand,"show lldp neighbors")
                        except:
                            logging.info(f"{device_ip:<25s}{hostname:<50s}{brand:<25s}不支持1.0.8802。同时也是无法解决。")
                    return
                command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802.1.1.2.1.3.7.1.3";process=self.demo1(command,2)
                first_line=process.stdout.readline().strip()
                try:
                    process.terminate();process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill();process.wait()
                if "No Such" in first_line:
                    if brand=="junos" or brand=="h3c":
                        logging.info(f"{device_ip:<25s}{hostname:<50s}{brand:<25s}没有本地接口设备。同时也是无法解决。")
                    else:
                        try:
                            try:
                                self.demo3(hostname,device_ip,brand,"show cdp neighbors")
                            except:
                                self.demo3(hostname,device_ip,brand,"show lldp neighbors")
                        except:
                            logging.info(f"{device_ip:<25s}{hostname:<50s}{brand:<25s}没有本地接口设备。同时也是无法解决。")
                    return
                basic_info=[hostname,device_ip,brand]
                for i in [2,4]:
                    command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802.1.1.2.1.3.{str(i)}";process=self.demo1(command,1)
                    info=self.demo2(process.stdout.strip().replace("\n",""),1);basic_info.append(info)
                    time.sleep(3)
                zd={}
                command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802.1.1.2.1.3.7.1.3";process=self.demo1(command,1)
                for line in process.stdout.strip().split("\n"):
                    id_=self.demo2(line,2);info=self.demo2(line,1)
                    zd[id_]=basic_info.copy()
                    zd[id_].append(info)
                time.sleep(3)
                command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802.1.1.2.1.3.7.1.4";process=self.demo1(command,1)
                for line in process.stdout.strip().split("\n"):
                    id_=self.demo2(line,2);info=self.demo2(line,1)
                    if id_ not in zd:
                        continue
                    zd[id_].append(info)
                time.sleep(3)
                for i in zd:
                    if len(zd[i])!=7:
                        zd[i].append("")
                command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802.1.1.2.1.4.1.1.5";process=self.demo1(command,2)
                first_line=process.stdout.readline().strip()
                try:
                    process.terminate();process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill();process.wait()
                if "No Such" in first_line:
                    if brand=="junos" or brand=="h3c":
                        logging.info(f"{device_ip:<25s}{hostname:<50s}{brand:<25s}没有远端接口设备。同时也是无法解决。")
                    else:
                        try:
                            try:
                                self.demo3(hostname,device_ip,brand,"show cdp neighbors")
                            except:
                                self.demo3(hostname,device_ip,brand,"show lldp neighbors")
                        except:
                            logging.info(f"{device_ip:<25s}{hostname:<50s}{brand:<25s}没有远端接口设备。同时也是无法解决。")
                    return
                lt=["5","7","8","9","10"]
                for i in range(len(lt)):
                    command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802.1.1.2.1.4.1.1.{lt[i]}";process=self.demo1(command,1)
                    for line in process.stdout.strip().replace("\n","").split("iso")[1:]:
                        id_=self.demo2(line,3);info=self.demo2(line,1)
                        if id_ not in zd:
                            continue
                        if len(zd[id_])==7+i:
                            zd[id_].append(info)
                    for _ in zd:
                        if len(zd[_])!=8+i:
                            zd[_].append("")
                    time.sleep(3)
                for i in zd.values():
                    self.result.append(i)
            elif brand in ["fenghuo","huarong","nokia"]:
                if brand=="fenghuo":
                    try:
                        self.demo3(hostname,device_ip,brand,"show lldp remote")
                    except:
                        logging.info(f"{device_ip:<25s}{hostname:<50s}{brand:<25s}不支持1.0.8802。同时也是无法解决。")
                elif brand=="huarong":
                    try:
                        self.demo3(hostname,device_ip,brand,"display lldp neighbor")
                    except:
                        logging.info(f"{device_ip:<25s}{hostname:<50s}{brand:<25s}不支持1.0.8802。同时也是无法解决。")
                elif brand=="nokia":
                    basic=[hostname,device_ip,brand,"",""]
                    zd={}
                    command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.3.6.1.4.1.6527.3.1.2.59.3.1.1.4";process=self.demo1(command,1)
                    for line in process.stdout.strip().split("\n"):
                        id_=self.demo2(line,3);info=self.demo2(line,1).split(",")[0]
                        basic_copy=basic.copy();basic_copy.append(info)
                        zd[id_]=basic_copy
                    for i in zd.keys():
                        zd[i].extend(["",""])
                    command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.3.6.1.4.1.6527.3.1.2.59.4.1.1.8";process=self.demo1(command,1)
                    if "No Such" in process.stdout.strip():
                        logging.info(f"{device_ip:<25s}{hostname:<50s}{brand:<25s}无法解决。")
                        return
                    for line in process.stdout.strip().split("\n"):
                        id_=self.demo2(line,4);info=self.demo2(line,1).split(",")[0]
                        if len(zd[id_])!=9:
                            zd[id_].append([])
                        zd[id_][-1].append(info)
                    for i in zd.keys():
                        if len(zd[i])!=9:
                            zd[i].append([])
                        zd[i].append("")
                    command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.3.6.1.4.1.6527.3.1.2.59.4.1.1.9";process=self.demo1(command,1)
                    for line in process.stdout.strip().split("\n"):
                        id_=self.demo2(line,4);info=self.demo2(line,1).split(".")[0]
                        if len(zd[id_])!=11:
                            zd[id_].append([])
                        zd[id_][-1].append(info)
                    for i in zd:
                        if len(zd[i])!=11:
                            zd[i].append([])
                        zd[i].append("")
                    for i in zd:
                        for j in range(len(zd[i][10])):
                            self.result.append(zd[i][:8]+[zd[i][8][j],"",zd[i][10][j],""])
            elif brand=="huawei":
                command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} iso.3.6.1.2.1.1.1.0";process=self.demo1(command,1)
                line=self.demo2(process.stdout.strip().replace("\n",""),1)
                if len(line)==0:
                    try:
                        self.demo3(hostname,device_ip,brand,"display lldp neighbor")
                    except:
                        logging.info(f"{device_ip:<25s}{hostname:<50s}{brand:<25s}没有型号。同时也是无法解决。")
                    return
                temp=line.split()
                if "-" in temp[0]:
                    kind=temp[0]
                elif "-" in temp[-1]:
                    kind=temp[-1]
                else:
                    try:
                        self.demo3(hostname,device_ip,brand,"display lldp neighbor")
                    except:
                        logging.info(f"{device_ip:<25s}{hostname:<50s}{brand:<25s}型号过老。同时也是无法解决。")
                    return
                version=re.findall(r'\((.*?)\)',line)[1]
                command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802";process=self.demo1(command,2)
                first_line=process.stdout.readline().strip()
                try:
                    process.terminate();process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill();process.wait()
                if "No Such" in  first_line:
                    try:
                        self.demo3(hostname,device_ip,brand,"display lldp neighbor")
                    except:
                        logging.info(f"{kind:<25s}{version:<50s}{device_ip:<25s}{hostname:<50s}{brand:<25s}不支持1.0.8802。同时也是无法解决。")
                    return
                command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802.1.1.2.1.3.7.1.3";process=self.demo1(command,2)
                first_line=process.stdout.readline().strip()
                try:
                    process.terminate();process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill();process.wait()
                if "No Such" in first_line:
                    try:
                        self.demo3(hostname,device_ip,brand,"display lldp neighbor")
                    except:
                        logging.info(f"{kind:<25s}{version:<50s}{device_ip:<25s}{hostname:<50s}{brand:<25s}没有本地接口设备。同时也是无法解决。")
                    return
                basic_info=[hostname,device_ip,brand]
                for i in [2,4]:
                    command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802.1.1.2.1.3.{str(i)}";process=self.demo1(command,1)
                    info=self.demo2(process.stdout.strip().replace("\n",""),1);basic_info.append(info)
                    time.sleep(3)
                zd={}
                command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802.1.1.2.1.3.7.1.3";process=self.demo1(command,1)
                for line in process.stdout.strip().split("\n"):
                    id_=self.demo2(line,2);info=self.demo2(line,1)
                    zd[id_]=basic_info.copy()
                    zd[id_].append(info)
                time.sleep(3)
                command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802.1.1.2.1.3.7.1.4";process=self.demo1(command,1)
                for line in process.stdout.strip().split("\n"):
                    id_=self.demo2(line,2);info=self.demo2(line,1)
                    if id_ not in zd:
                        continue
                    zd[id_].append(info)
                time.sleep(3)
                for i in zd:
                    if len(zd[i])!=7:
                        zd[i].append("")
                command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802.1.1.2.1.4.1.1.5";process=self.demo1(command,2)
                first_line=process.stdout.readline().strip()
                try:
                    process.terminate();process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill();process.wait()
                if "No Such" in first_line:
                    try:
                        self.demo3(hostname,device_ip,brand,"display lldp neighbor")
                    except:
                        logging.info(f"{kind:<25s}{version:<50s}{device_ip:<25s}{hostname:<50s}{brand:<25s}没有远端接口设备。同时也是无法解决。")
                    return
                lt=["5","7","8","9","10"]
                for i in range(len(lt)):
                    command=f"snmpwalk -v 2c -c QAZXSWedc {device_ip} 1.0.8802.1.1.2.1.4.1.1.{lt[i]}";process=self.demo1(command,1)
                    for line in process.stdout.strip().replace("\n","").split("iso")[1:]:
                        id_=self.demo2(line,3);info=self.demo2(line,1)
                        if id_ not in zd:
                            continue
                        if len(zd[id_])==7+i:
                            zd[id_].append(info)
                    for _ in zd:
                        if len(zd[_])!=8+i:
                            zd[_].append("")
                    time.sleep(3)
                for i in zd.values():
                    self.result.append(i)
        except Exception as e:
            logging.error(f"{device_ip:<25s}{hostname:<50s}{brand:<25s}{e}")
        
    def demo1(self,command,mode):
        if mode==1:
            process=subprocess.run(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
        elif mode==2:
            process=subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1
            )
        return process

    def demo2(self,info,mode):
        if mode==1:
            if ":" not in info:
                return ""
            else:
                info=info[info.index(":")+1:].strip()
                while len(info)>0 and (info[0]=="\"" or info[0]=="'" or info[0]==" "):
                    info=info[1:]
                while len(info)>0 and (info[-1]=="\"" or info[-1]=="'" or info[-1]==" "):
                    info=info[:-1]
                return info
        else:
            temp1=info.split()[0]
            temp2=temp1.split(".")[-(mode-1)]
            return temp2

    def demo3(self,hostname,device_ip,brand,command):
        url='http://10.213.136.111:40061/network_app/distribute_config/exec_cmd/'
        data={
            "device_hostname":hostname,
            "operator":"devops",
            "is_edit":False,
            "cmd":command
        }
        response=requests.post(url,json=data)
        temp=response.json()["data"]["cmd_result"].split("\n")
        lt=[]
        if brand=="cisco":
            temp=temp[temp.index("")+2:]
            temp=temp[:temp.index("")]
            index=0
            while index<len(temp):
                temp_temp=temp[index].split()
                x=temp_temp[0]
                if "(" in x:
                    x=x[:x.index("(")]
                if "-"==x[-1] or "."==x[-1]:
                    x=x[:-1]
                if hostname in x:
                    break
                if len(temp_temp)==1:
                    temp_temp=temp[index+1].split()
                    y=temp_temp[0]
                    if "/" not in y:
                        y+=temp_temp[1]
                    index+=2
                else:
                    y=temp_temp[1]
                    index+=1
                if "[" in y:
                    y=y[:y.index("[")]
                z=temp_temp[-1]
                if "Gig" in temp_temp[-2]:
                    z="Gig"+z
                lt.append([hostname,device_ip,brand,"","",y,"","",z,"",x,""])
        elif brand=="fenghuo":
            temp=temp[3:]
            for i in temp:
                temp_temp=i.split()
                x=temp_temp[0]
                if hostname in x:
                    break
                if "SN:" in i:
                    y=temp_temp[4]
                    z=""
                    for _ in range(5,len(temp_temp)):
                        z+=temp_temp[_]
                else:
                    temp_temp_temp=temp_temp[4:-1]
                    y=""
                    for _ in temp_temp_temp:
                        y+=_
                    z=temp_temp[-1]
                lt.append([hostname,device_ip,brand,"","",x,"","",y,"",z,""])
        elif brand=="huarong" or brand=="huawei":
            temp_index=[i for i,s in enumerate(temp) if "Chassis ID" in s]
            chassis_ids=[temp[i][temp[i].index(":")+1:].strip() for i in temp_index]
            temp_index=[i for i,s in enumerate(temp) if "Port ID" in s and "subtype" not in s and "type" not in s]
            port_ids=[temp[i][temp[i].index(":")+1:].strip() for i in temp_index]
            temp_index=[i for i,s in enumerate(temp) if "Port description" in s]
            port_descriptions=[temp[i][temp[i].index(":")+1:].strip() for i in temp_index]
            if len(port_descriptions)!=len(chassis_ids):
                port_descriptions=[""]*len(chassis_ids)
            temp_index=[i for i,s in enumerate(temp) if "System name" in s]
            system_names=[temp[i][temp[i].index(":")+1:].strip() for i in temp_index]
            temp_index=[i for i,s in enumerate(temp) if "System description" in s]
            system_descriptions=[]
            for i in temp_index:
                temp_s=temp[i][temp[i].index(":")+1:].strip()
                j=i+1
                while True:
                    if "System capabilities supported" in temp[j]:
                        break
                    else:
                        temp_s=temp_s+" "+temp[j].strip()
                    j+=1
                system_descriptions.append(temp_s)
            temp_index=[i for i,s in enumerate(temp) if "has" in s and "neighbor(s)" in s]
            local_interfaces=[]
            for i in temp_index:
                temp_temp=temp[i].split()
                number=int(temp_temp[2])
                for i in range(number):
                    local_interfaces.append(temp_temp[0].strip())
            for i in range(len(local_interfaces)):
                temp_temp=[hostname,device_ip,brand,"","",local_interfaces[i],"",chassis_ids[i],port_ids[i],port_descriptions[i],system_names[i],system_descriptions[i]]
                for j in range(len(temp_temp)):
                    if temp_temp[j]=="-" or temp_temp[j]=="--" or temp_temp[j]=="---":
                        temp_temp[j]=""
                lt.append(temp_temp)
        for i in lt:
            self.result.append(i)

    def demo4(self,s,brand,hostname="",mode=None):
        if brand=="cisco":
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
            if "Bundle-Ether" not in s and "BE" in s:
                s=s.replace("BE","Bundle-Ether")
                return s
        elif brand=="fenghuo":
            if "gigaethernet" not in s and "ge" in s:
                s=s.replace("ge","gigaethernet")
            if mode==0:
                s=s[:s.index("/")-1]+" "+s[s.index("/")-1:]
                if hostname in [f"CNIQN-POD235-F58-OOB-{i:02d}" for i in range(1,22)]+["CNIQN-POD235-F58-OOB-28"]:
                    if "gigaethernet" in s:
                        s=s.replace("gigaethernet","GigaEthernet")
                    temp=""
                    for i in s:
                        if i==" ":
                            continue
                        temp+=i
                    s=temp
            elif mode==1:
                if "gigaethernet" in s or "mgt-eth" in s:
                    s=s[:s.index("/")-1]+" "+s[s.index("/")-1:]
        return s

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