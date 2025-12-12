import os
import logging
from logging.handlers import RotatingFileHandler
from bson import ObjectId
from connect import Connect_Mysql,Connect_Mongodb
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
        self.pipeline=[
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
        self.result=[]

    def run(self):
        self.run1()
        self.run2()
        self.run3()
        self.run4()
        temp=[]
        for i in self.result:
            if i[3]=="vmk0":
                continue
            temp.append(i)
        columns=["hostname","device_ip","brand","nic","function","sysname","port"]
        columns_str=', '.join(columns);values_placeholders=', '.join(['%s']*len(columns))
        update_clause=', '.join([f"{column} = VALUES({column})" for column in columns if column not in ["hostname","nic"]])
        sql=f'''
            INSERT INTO interface_between_nic ({columns_str}) VALUES ({values_placeholders}) ON DUPLICATE KEY UPDATE {update_clause};
        '''
        db_mysql=Connect_Mysql(self.config2)
        cursor=db_mysql.client.cursor()
        values_list=[tuple(row) for row in temp]
        cursor.executemany(sql,values_list)
        db_mysql.client.commit()

    def run1(self):
        db_mysql=Connect_Mysql(self.config2)
        data=db_mysql.get_table_data("interface","select hostname,name,mac_address from interface").values.tolist()
        zd={}
        for i in data:
            if i[2].lower()=="nan" or i[2].lower()=="null" or i[2].lower()=="none" or i[2]==None or i[2]=="" or i[2]=="-" or i[2]=="--" or i[2]=="---":
                continue
            if "CL" not in i[0] and "FE" not in i[0]:
                continue
            if "vtep" in i[1]:
                continue
            temp=i[2].split("|")
            for j in temp:
                if j not in zd:
                    zd[j]=[]
                zd[j].append(f"{i[0]}|{i[1]}")
        pipeline=self.pipeline.copy()
        pipeline[0]["$match"]["device_server_group"]=ObjectId("5ec8c70a94285cfd9cacee91")
        db_mongo=Connect_Mongodb(self.config1)
        data=pd.DataFrame(list(db_mongo.db.cds_ci_att_value_server.aggregate(pipeline))).astype(str).values.tolist()
        def fc(info):
            hostname,device_ip,brand=info[0],info[1],info[2].lower()
            if hostname.lower()=="nan" or hostname.lower()=="null" or hostname.lower()=="none" or hostname==None or hostname=="-" or hostname=="--" or hostname=="---":
                return
            if "." not in device_ip:
                return
            client=SSH_Server(hostname,device_ip,brand).client
            if client==None:
                return
            for _ in range(5):
                try:
                    stdin,stdout,stderr=client.exec_command("esxcfg-vmknic -l |grep IPv4")
                    output=stdout.read().decode('utf-8').strip()
                    lt=[]
                    for i in output.split("\n"):
                        basic_info=[hostname,device_ip,brand]
                        temp=i.split()
                        basic_info.extend([temp[0],temp[1]])
                        for j in range(2,len(temp)):
                            if len(temp[j].split(":"))==6:
                                if temp[j] in zd:
                                    for k in zd[temp[j]]:
                                        basic_info_copy=basic_info.copy()
                                        temp_temp=k.split("|")
                                        x=None
                                        for z in hostname.split("-"):
                                            if "POD" in z:
                                                x=z
                                                break
                                        y=None
                                        for z in temp_temp[0].split("-"):
                                            if "POD" in z:
                                                y=z
                                                break
                                        if x!=None and y!=None and x!=y:
                                            continue
                                        basic_info_copy.extend([temp_temp[0],temp_temp[1]])
                                        lt.append(basic_info_copy)
                                break
                    for i in lt:
                        self.result.append(i)
                    break
                except:
                    time.sleep(5)
            else:
                logging.error(f"{hostname:<50s}{device_ip:<25s}{brand:<25s}执行命令失败。")
            client.close()
        with ThreadPoolExecutor(max_workers=25) as executor:
            pool=[]
            for i in data:
                pool.append(executor.submit(fc,[i[1],i[2],i[3]]))
            for task in as_completed(pool):
                task.result()

    def run2(self):
        pipeline=self.pipeline.copy()
        pipeline[0]["$match"]["device_server_group"]={"$in":[ObjectId("5ec8c70a94285cfd9cacee92"),ObjectId("5ec8c70a94285cfd9cacee95")]}
        db_mongo=Connect_Mongodb(self.config1)
        data=pd.DataFrame(list(db_mongo.db.cds_ci_att_value_server.aggregate(pipeline))).astype(str).values.tolist()
        def fc(info):
            hostname,device_ip,brand=info[0],info[1],info[2].lower()
            if hostname.lower()=="nan" or hostname.lower()=="null" or hostname.lower()=="none" or hostname==None or hostname=="-" or hostname=="--" or hostname=="---":
                return
            for i in ["SDS","MDM","EBS"]:
                if i in hostname:
                    break
            else:
                return
            if "." not in device_ip:
                return
            client=SSH_Server(hostname,device_ip,brand).client
            if client==None:
                return
            try:
                stdin,stdout1,stderr=client.exec_command("apt-get install -y lldpd")
                output1=stdout1.read().decode('utf-8').strip()
                stdin,stdout2,stderr=client.exec_command("systemctl start lldpd")
                output2=stdout2.read().decode('utf-8').strip()
                stdin,stdout3,stderr=client.exec_command("lldpcli show neighbors")
                output3=stdout3.read().decode('utf-8').strip()
                client.close()
            except:
                logging.info(f"{hostname:<50s}{brand:<25s}{device_ip:<25s}我不太清楚。")
                client.close()
                return
            temp=output3.split("\n")
            lt_interfaces=[];lt_systems=[];lt_ports=[]
            for i in temp:
                temp_temp=i.split()
                if "Interface" in i and "LLDP" in i:
                    lt_interfaces.append(temp_temp[1][:-1].strip())
                if "SysName" in i:
                    lt_systems.append(temp_temp[1].strip())
                if "PortID" in i:
                    lt_ports.append(temp_temp[-1].strip())
            for i in range(len(lt_interfaces)):
                basic_info=[hostname,device_ip,brand,lt_interfaces[i],"",lt_systems[i],lt_ports[i]]
                self.result.append(basic_info)
        with ThreadPoolExecutor(max_workers=25) as executor:
            pool=[]
            for i in data:
                pool.append(executor.submit(fc,[i[1],i[2],i[3]]))
            for task in as_completed(pool):
                task.result()

    def run3(self):
        pipeline=self.pipeline.copy()
        pipeline[0]["$match"]["device_server_group"]=ObjectId("5ec8c70a94285cfd9cacee94")
        db_mongo=Connect_Mongodb(self.config1)
        hostnames=set(pd.DataFrame(list(db_mongo.db.cds_ci_att_value_server.aggregate(pipeline))).astype(str)["hostname"].values.tolist())
        db_mysql=Connect_Mysql(self.config2)
        data=db_mysql.get_table_data("interface","select hostname,name from interface").values.tolist()
        interfaces=set()
        for i in data:
            interfaces.add(f"{i[0]}|{i[1]}")
        config={
            "connection":{
                "TIMES":5,
                "TIME":0.5
            },
            "mysql":{
                "HOST":"",
                "PORT":0,
                "USERNAME":"",
                "PASSWORD":"",
                "DATABASE":"cds_bmscontrol"
            }
        }
        database_list=[
            ["10.198.0.251"  ,3306,"pod11_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod12_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.128.92.199" ,3306,"pod17_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.213.103.101",3306,"pod21_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod25_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.200.43.3"   ,3306,"pod27_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.200.49.3"   ,3306,"pod29_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.198.52.3"   ,3306,"pod30_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod31_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.197.31.29"  ,3306,"pod32_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod33_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod35_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.198.23.109" ,3306,"pod38_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod42_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.213.20.3"   ,3306,"pod43_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod44_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod45_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod46_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod47_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.198.80.3"   ,3306,"pod49_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.212.228.3"  ,3306,"pod50_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod53_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod55_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod57_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.200.81.171" ,3306,"pod58_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.128.219.239",3306,"pod61_ro" ,"hT4arhlveP$z3!9p0pV7"],
            ["10.192.40.103" ,3306,"pod62_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.200.227.240",3306,"pod63_ro" ,"hT4arhlveP$z3!9p0pV7"],
            ["10.191.3.83"   ,3306,"pod66_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.190.3.83"   ,3306,"pod67_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod69_ro" ,"U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod203_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod204_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod205_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod206_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod207_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod208_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod209_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod210_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod211_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod212_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod213_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod215_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod216_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod217_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod218_ro","U9ikmdnw#sfkelp"     ],
            ["10.212.139.216",3306,"pod219_ro","U9ikmdnw#sfkelp"     ],
            ["10.212.144.214",3306,"pod220_ro","U9ikmdnw#sfkelp"     ],
            ["10.212.152.1"  ,3306,"pod221_ro","U9ikmdnw#sfkelp"     ],
            ["10.212.184.214",3306,"pod223_ro","U9ikmdnw#sfkelp"     ],
            ["10.212.220.8"  ,3306,"pod224_ro","U9ikmdnw#sfkelp"     ],
            ["10.212.200.3"  ,3306,"pod225_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod226_ro","U9ikmdnw#sfkelp"     ],
            ["10.212.48.1"   ,3306,"pod229_ro","U9ikmdnw#sfkelp"     ],
            ["10.212.59.3"   ,3306,"pod230_ro","U9ikmdnw#sfkelp"     ],
            ["10.191.2.203"  ,3306,"pod231_ro","U9ikmdnw#sfkelp"     ],
            ["10.212.108.3"  ,3306,"pod234_ro","U9ikmdnw#sfkelp"     ],
            ["10.212.116.8"  ,3306,"pod235_ro","U9ikmdnw#sfkelp"     ],
            ["10.212.124.3"  ,3306,"pod236_ro","U9ikmdnw#sfkelp"     ],
            ["10.212.244.8"  ,3306,"pod238_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod240_ro","U9ikmdnw#sfkelp"     ],
            ["10.13.103.180" ,6033,"pod244_ro","U9ikmdnw#sfkelp"     ]
        ]
        def fc(config_i):
            try:
                db_mysql_temp=Connect_Mysql(config_i)
                sql="""
                select host_nic.*,switch.server_name as sw_name,switch.server as sw_server
                        from (select h.id as host_id,h.name as host_name,h.server as host_server,nc.device_no,
                            nc.mac as nc_mac,nc.port_ip,nc.port_name,nc.switch_id
                        from host h left join network_card nc on h.id=nc.host_id
                        where h.state !='deleted' and h.host_type !='manage'
                    ) as host_nic left join switch on host_nic.switch_id=switch.id
                    WHERE
                    host_nic.host_name not like "%-NAS-%"
                    order by host_nic.host_server,host_nic.device_no,host_nic.port_ip;
                """
                cursor=db_mysql_temp.client.cursor()
                cursor.execute(sql)
                columns=[desc[0] for desc in cursor.description]
                data=cursor.fetchall()
                df=pd.DataFrame(data,columns=columns)[["host_name","host_server","nc_mac","port_name","sw_name","sw_server"]]
                for i in df.values.tolist():
                    if i[0] not in hostnames:
                        continue
                    x=i[3]
                    if "et-" not in x and "et" in x:
                        x=x.replace("et","et-")
                    if "Ethernet" not in x and "Eth" in x:
                        x=x.replace("Eth","Ethernet")
                    if "Ethernet-" in x:
                        x=x.replace("Ethernet-","Ethernet")
                    if f"{i[4]}|{x}" not in interfaces:
                        continue
                    if "POD235" in i[4]:
                        if "F69" in i[4]:
                            if "GE" in x:
                                x=x.replace("GE","gigaethernet ")
                    temp=[i[0],i[1],"",i[2],"裸金属计算",i[4],x]
                    self.result.append(temp)
            except Exception as e:
                logging.info(f'{config_i}连不上。{e}')
        for i in database_list:
            if i[0]=="10.13.103.180":
                continue
            temp=config.copy()
            temp["mysql"]["HOST"]=i[0]
            temp["mysql"]["PORT"]=i[1]
            temp["mysql"]["USERNAME"]=i[2]
            temp["mysql"]["PASSWORD"]=i[3]
            fc(temp)

    def run4(self):
        jh=set()
        for i in self.result:
            jh.add((i[5],i[6]))
        db_mysql=Connect_Mysql(self.config2)
        df=db_mysql.get_table_data("interface_to_interface","select * from interface_to_interface")
        data1=df[["hostname","lldpLocPortId","lldpRemSysName","lldpRemPortDesc"]].values.tolist()
        for i in data1:
            if "JSDS" in i[2]:
                continue
            for j in ["SDS","EBS","MDM","CLU"]:
                if j in i[2]:
                    break
            else:
                continue
            if "cswitch" in i[3]:
                continue
            i[3]=i[3].split()[-1]
            if (i[0],i[1]) in jh:
                continue
            self.result.append([i[2],"","",i[3],"",i[0],i[1]])
        data2=df[["hostname","lldpLocPortId","lldpRemSysName","lldpRemPortId","lldpRemPortDesc"]].values.tolist()
        for i in data2:
            if "CLU" not in i[2]:
                continue
            if "cswitch" not in i[4]:
                continue
            i[3]=i[3].split()
            if len(i[3])!=6:
                temp=i[3][0].split("-")
                i[3]=[]
                for j in temp:
                    i[3].append(j[:2])
                    i[3].append(j[2:])
            i[3]=":".join(i[3])
            if (i[0],i[1]) in jh:
                continue
            self.result.append([i[2],"","",i[3],"",i[0],i[1]])

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