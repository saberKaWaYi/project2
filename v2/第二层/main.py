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

handler=get_rotating_handler("main.log")
logging_main=logging.getLogger("main")
logging_main.setLevel(logging.INFO)
logging_main.addHandler(handler)

from sql_demo import Sql_tool
from datetime import datetime
from connect import Connect_Mysql,Connect_Nebula
import time
import pandas as pd

class Run:

    def __init__(self,config1,config2):
        self.config1=config1
        self.config2=config2
        self.m=Sql_tool()
        self.wait_time=15
        temp=datetime.today()
        self.name=f"G_{temp.year:04d}_{temp.month:02d}_{temp.day:02d}"
        self.db_mysql=Connect_Mysql(self.config1)
        self.db_mysql_client=self.db_mysql.client.cursor()
        self.temp=None

    def init(self):
        conn=Connect_Nebula(self.config2)
        conn.open_nebula()
        client=conn.client
        try:
            lt=client.execute("show spaces;").as_data_frame()["Name"].values.tolist()
            lt.sort()
            if len(lt)>14:
                self.m.drop_space(client,lt[0])
                time.sleep(self.wait_time)
        except:
            pass
        flag=True
        lt=[
            "area","between_area_and_country","between_city_and_data_center","between_country_and_city","between_data_center_and_room","between_network_and_interface",
            "between_rack_and_network","between_rack_and_server","between_rack_and_storage","between_room_and_rack","between_server_and_nic","city","country","data_center",
            "interface","network","nic","rack","room","server","storage"
        ]
        for i in lt:
            self.db_mysql_client.execute(f"select count(*) as count from topu.{i};")
            result=self.db_mysql_client.fetchone()["count"]
            if result==0:
                flag=False
                break
        else:
            self.m.create_space(client,self.name)
            time.sleep(self.wait_time)
        conn.close_nebula()
        return flag
    
    def create_nodes(self,table_name,key_name_list,df=None,chunk_size=1000):
        if df is None:
            df=self.db_mysql.get_table_data("",f"select * from topu.{table_name};")
        conn=Connect_Nebula(self.config2)
        conn.open_nebula()
        client=conn.client
        client.execute(f"USE {self.name};")
        self.m.create_tag(client,table_name,{key:"string" for key in df.columns.to_list() if key not in key_name_list})
        time.sleep(self.wait_time)
        self.m.create_nodes(client,table_name,key_name_list,df,chunk_size)
        time.sleep(self.wait_time)
        conn.close_nebula()
        logging_main.info(f"建立节点{table_name}成功了。")
    
    def create_network_nodes(self):
        self.create_nodes("network",["hostname"])

    def create_server_nodes(self):
        self.create_nodes("server",["hostname"])

    def create_interface_nodes(self):
        self.create_nodes("interface",["hostname","name"],chunk_size=10000)

    def create_nic_nodes(self):
        df1=self.db_mysql.get_table_data("","select * from topu.nic;").values.tolist()
        df2=self.db_mysql.get_table_data("","select * from topu.between_interface_and_nic;")[["server_hostname","server_ip","server_brand","nic"]].values.tolist()
        jh1=set([(i[0],i[3]) for i in df1])
        jh2=set(self.db_mysql.get_table_data("","select hostname from topu.server;")["hostname"].values.tolist())
        for i in df2:
            if (i[0],i[-1]) in jh1:
                continue
            if i[0] not in jh2:
                continue
            df1.append([i[0],i[1],i[2],i[3],i[3],"接口",""])
        df=pd.DataFrame(df1,columns=["hostname","ip","brand","nic","mac_address","type","description"])
        self.create_nodes("nic",["hostname","nic"],df,chunk_size=10000)
        self.temp=set([(i[0],i[3]) for i in df.values.tolist()])

    def create_edges(self,edge_type,relationship,chunk_size=1000):
        conn=Connect_Nebula(self.config2)
        conn.open_nebula()
        client=conn.client
        client.execute(f"USE {self.name};")
        self.m.create_edge_type(client,edge_type)
        time.sleep(self.wait_time)
        self.m.create_edges(client,edge_type,relationship,chunk_size)
        time.sleep(self.wait_time)
        conn.close_nebula()
        logging_main.info(f"建立边{edge_type}成功了。")

    def create_between_network_and_interface_edges(self):
        temp=self.db_mysql.get_table_data("","select * from topu.between_network_and_interface;").values.tolist()
        relationship1=[];relationship2=[]
        for i in temp:
            relationship1.append([i[0],f"{i[0]}|{i[1]}","网络"])
            relationship2.append([f"{i[0]}|{i[1]}",i[0],"网络"])
        self.create_edges("network_to_interface",relationship1,chunk_size=10000)
        self.create_edges("interface_to_network",relationship2,chunk_size=10000)

    def create_between_interface_and_interface_edges(self):
        temp=self.db_mysql.get_table_data("","select lldpLocSysName,lldpRemSysName,lldpLocPortId,lldpRemPortId from topu.between_interface_and_interface;")[["lldpLocSysName","lldpRemSysName","lldpLocPortId","lldpRemPortId"]].values.tolist()
        jh=set([(i[0],i[1]) for i in self.db_mysql.get_table_data("","select hostname,name from topu.interface;")[["hostname","name"]].values.tolist()])
        relationship=[]
        for i in temp:
            if (i[0],i[2]) in jh and (i[1],i[3]) in jh:
                relationship.append([f"{i[0]}|{i[2]}",f"{i[1]}|{i[3]}","网络"])
                relationship.append([f"{i[1]}|{i[3]}",f"{i[0]}|{i[2]}","网络"])
        self.create_edges("interface_to_interface",relationship,chunk_size=10000)

    def create_between_server_and_nic_edges(self):
        relationship1=[];relationship2=[]
        for i in self.temp:
            relationship1.append([i[0],f"{i[0]}|{i[1]}","网络"])
            relationship2.append([f"{i[0]}|{i[1]}",i[0],"网络"])
        self.create_edges("server_to_nic",relationship1,chunk_size=10000)
        self.create_edges("nic_to_server",relationship2,chunk_size=10000)

    def create_between_interface_and_nic_edges(self):
        temp=self.db_mysql.get_table_data("","select server_hostname,nic,network_hostname,interface from topu.between_interface_and_nic;")[["server_hostname","nic","network_hostname","interface"]].values.tolist()
        jh1=set([(i[0],i[1]) for i in self.db_mysql.get_table_data("","select hostname,name from topu.interface;")[["hostname","name"]].values.tolist()])
        jh2=self.temp
        relationship1=[];relationship2=[]
        for i in temp:
            if (i[0],i[1]) in jh2 and (i[2],i[3]) in jh1:
                relationship1.append([f"{i[2]}|{i[3]}",f"{i[0]}|{i[1]}","网络"])
                relationship2.append([f"{i[0]}|{i[1]}",f"{i[2]}|{i[3]}","网络"])
        self.create_edges("interface_to_nic",relationship1,chunk_size=10000)
        self.create_edges("nic_to_interface",relationship2,chunk_size=10000)
    
    def run(self):
        self.create_network_nodes()
        self.create_server_nodes()
        self.create_interface_nodes()
        self.create_nic_nodes()
        self.create_between_network_and_interface_edges()
        self.create_between_interface_and_interface_edges()
        self.create_between_server_and_nic_edges()
        self.create_between_interface_and_nic_edges()

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
        "nebula":{
            "HOST":"10.216.142.31",
            "PORT":9669,
            "USERNAME":"root",
            "PASSWORD":"cds-cloud@2017",
            "MIN_CONNECTION_POOL_SIZE":1,
            "MAX_CONNECTION_POOL_SIZE":260
        }
    }
    m=Run(config1,config2)
    if m.init():
        m.run()