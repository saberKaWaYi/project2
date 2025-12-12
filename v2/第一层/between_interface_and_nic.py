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

handler=get_rotating_handler("between_interface_and_nic.log")
logging_between_interface_and_nic=logging.getLogger("between_interface_and_nic")
logging_between_interface_and_nic.setLevel(logging.INFO)
logging_between_interface_and_nic.addHandler(handler)

from connect import Connect_Mysql
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor,as_completed
from ssh import SSH_Server
import pandas as pd

class Run:

    def __init__(self,config):
        self.config=config
        self.conn=Connect_Mysql(self.config)
        self.conn_client=self.conn.client.cursor()
        self.time=datetime.now()
        self.result=[];self.lock1=threading.Lock()
        self.errors=[];self.lock2=threading.Lock()

    def create_table(self):
        sql='''
        create table if not exists topu.between_interface_and_nic (
            server_hostname VARCHAR(100),
            server_ip VARCHAR(100),
            server_brand VARCHAR(100),
            nic VARCHAR(100),
            network_hostname VARCHAR(100),
            network_ip VARCHAR(100),
            network_brand VARCHAR(100),
            interface VARCHAR(100)
        );
        '''
        self.conn_client.execute(sql)
        self.conn.client.commit()

    def truncate_table(self):
        sql='''
        truncate table topu.between_interface_and_nic;
        '''
        self.conn_client.execute(sql)
        self.conn.client.commit()

    def main1(self,s):
        nics=self.conn.get_table_data("",f"select hostname,ip,brand,name,mac_address from topu.nic where type='{s}'")[["hostname","ip","brand","name","mac_address"]].values.tolist()
        dict_1=dict(zip([i[-1] for i in nics],[[i[0],i[1],i[2],i[3]] for i in nics]))
        interfaces=self.conn.get_table_data("","select * from topu.interface where mac_address is not null and name NOT LIKE '%vtep%';")[["hostname","name","mac_address"]].values.tolist()
        dict_2={}
        for i in interfaces:
            for j in i[-1].split("|"):
                if j not in dict_2:
                    dict_2[j]=[]
                dict_2[j].append((i[0],i[1]))
        networks=self.conn.get_table_data("","select hostname,ip,brand from topu.network")[["hostname","ip","brand"]].values.tolist()
        dict_3=dict(zip([i[0] for i in networks],[[i[1],i[2]] for i in networks]))
        for i in dict_1:
            if i not in dict_2:
                continue
            for j in dict_2[i]:
                x,y=None,None
                for k in dict_1[i][0].split("-"):
                    if "POD" in k:
                        x=k
                        break
                for k in j[0].split("-"):
                    if "POD" in k:
                        y=k
                if not x and not y and x!=y:
                    continue
                self.result.append((dict_1[i][0],dict_1[i][1],dict_1[i][2],dict_1[i][3],j[0],dict_3[j[0]][0],dict_3[j[0]][1],j[1]))

    def main2(self):
        servers=self.conn.get_table_data("","select hostname,ip,brand,name from topu.nic where type='存储'")[["hostname","ip","brand","name"]].values.tolist()
        zd=set((i[0],i[-1]) for i in servers)
        jh=set()
        networks=self.conn.get_table_data("","select hostname,ip,brand from topu.network")[["hostname","ip","brand"]].values.tolist()
        zd_0=dict(zip([i[0] for i in networks],[[i[1],i[2]] for i in networks]))
        interfaces=self.conn.get_table_data("","select hostname,name from topu.interface")[["hostname","name"]].values.tolist()
        jh_0=set((i[0],i[1]) for i in interfaces)
        def fc(hostname,ip,brand):
            client=SSH_Server(hostname,ip,brand).client
            if client==None:
                with self.lock2:
                    self.errors.append((hostname,ip,brand,"登录不上",self.time,"between_interface_and_nic.py"))
                logging_between_interface_and_nic.error(f"{hostname},{ip},{brand}。登录不上。")
                return
            try:
                stdin,stdout1,stderr=client.exec_command("apt-get install -y lldpd")
                output1=stdout1.read().decode('utf-8').strip()
                stdin,stdout2,stderr=client.exec_command("systemctl start lldpd")
                output2=stdout2.read().decode('utf-8').strip()
                stdin,stdout3,stderr=client.exec_command("lldpcli show neighbors")
                output3=stdout3.read().decode('utf-8').strip()
                nics=[];hostnames=[];interfaces=[]
                for line in output3.strip().split("\n"):
                    if "Interface:" in line and "LLDP" in line:
                        nics.append(line.split()[1][:-1])
                    if "SysName:" in line and len(line.split())==2:
                        hostnames.append(line.split()[1])
                    if "PortID:" in line and "ifname" in line:
                        interfaces.append(line.split()[2])
                len_=len(nics)
                for i in range(len_):
                    if (hostname,nics[i]) not in zd:
                        continue
                    if (hostnames[i],interfaces[i]) not in jh_0:
                        continue
                    with self.lock1:
                        self.result.append((hostname,ip,brand,nics[i],hostnames[i],zd_0[hostnames[i]][0],zd_0[hostnames[i]][1],interfaces[i]))
            except Exception as e:
                with self.lock2:
                    self.errors.append((hostname,ip,brand,str(e),self.time,"between_interface_and_nic.py"))
                logging_between_interface_and_nic.error(f"{hostname},{ip},{brand}。{str(e)}。")
            client.close()
        with ThreadPoolExecutor(max_workers=25) as executor:
            pool=[]
            for server in servers:
                if server[0] in jh:
                    continue
                pool.append(executor.submit(fc,server[0],server[1],server[2]))
                jh.add(server[0])
            for task in as_completed(pool):
                task.result()

    def main3(self):
        networks=self.conn.get_table_data("","select hostname,ip,brand from topu.network")[["hostname","ip","brand"]].values.tolist()
        networks=dict(zip([i[0] for i in networks],[[i[1],i[2]] for i in networks]))
        servers=self.conn.get_table_data("","select hostname,in_band_ip,brand2 from topu.server")[["hostname","in_band_ip","brand2"]].values.tolist()
        servers=dict(zip([i[0] for i in servers],[[i[1],i[2]] for i in servers]))
        interfaces=self.conn.get_table_data("","select hostname,name from topu.interface")[["hostname","name"]].values.tolist()
        interfaces=set((i[0],i[1]) for i in interfaces)
        config={
            "connection":{
                "TIMES":3,
                "TIME":1
            },
            "mysql":{
                "HOST":"",
                "PORT":"",
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
                cursor=db_mysql_temp.client.cursor()
                cursor.execute(f"use {config_i['mysql']['DATABASE']};")
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
                cursor.execute(sql)
                columns=[desc[0] for desc in cursor.description]
                data=cursor.fetchall()
                df=pd.DataFrame(data,columns=columns)[["host_name","host_server","nc_mac","port_name","sw_name","sw_server"]].values.tolist()
                for i in df:
                    if i[0] not in servers:
                        continue
                    if i[4] not in networks:
                        continue
                    if (i[4],i[3]) not in interfaces:
                        x=i[3]
                        if "Ethernet" not in x and "Eth" in x:
                            x=x.replace("Eth","Ethernet")
                        if "et-" not in x and "et" in x:
                            x=x.replace("et","et-")
                        if "GE" not in x and "G" in x:
                            x=x.replace("G","GE")
                        if "POD235" in i[4] and "F69" in i[4] and "GE" in x:
                            x=x.replace("GE","gigaethernet ")
                        if (i[4],x) not in interfaces:
                            continue
                        i[3]=x
                    with self.lock1:
                        self.result.append((i[0],servers[i[0]][0],servers[i[0]][1],i[2],i[4],networks[i[4]][0],networks[i[4]][1],i[3]))
            except Exception as e:
                with self.lock2:
                    self.errors.append(("","","",str(e),self.time,"between_interface_and_nic.py"))
                logging_between_interface_and_nic.error(f"{str(e)}。")
        for i in database_list:
            if i[0]=="10.13.103.180":
                continue
            temp=config.copy()
            temp["mysql"]["HOST"]=i[0]
            temp["mysql"]["PORT"]=i[1]
            temp["mysql"]["USERNAME"]=i[2]
            temp["mysql"]["PASSWORD"]=i[3]
            fc(temp)

    def main(self):
        self.main1("物理")
        self.main1("虚拟")
        self.main2()
        self.main3()

    def insert_data(self):
        sql='''
        insert into topu.between_interface_and_nic (server_hostname,server_ip,server_brand,nic,network_hostname,network_ip,network_brand,interface) values (%s,%s,%s,%s,%s,%s,%s,%s);
        '''
        self.conn_client.executemany(sql,self.result)
        self.conn.client.commit()
        sql='''
        insert into cds_report.collect_lldp_from_server (hostname,ip,brand,info,time,file) values (%s,%s,%s,%s,%s,%s)
        '''
        self.conn_client.executemany(sql,self.errors)
        self.conn.client.commit()

    def run(self):
        self.create_table()
        self.truncate_table()
        self.main()
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