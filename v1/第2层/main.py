import os
import logging
from logging.handlers import RotatingFileHandler
from create import Create
from datetime import datetime
from connect import Connect_Nebula,Connect_Mysql
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor,as_completed

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
        self.m=Create()
        temp=datetime.today()
        self.name=f"G_{temp.year:04d}_{temp.month:02d}_{temp.day:02d}"

    def init(self):
        conn=Connect_Nebula(self.config2)
        conn.open_nebula()
        client=conn.client
        try:
            lt=client.execute("show spaces").as_data_frame()["Name"].values.tolist()
            lt.sort()
            if len(lt)>15:
                self.m.drop_space(client,lt[0])
                time.sleep(15)
        except:
            pass
        flag=True
        lt=["network","server","storage","interface","interface_to_interface","podx_clux_all_ipv4","interface_between_nic","area","country","city",\
            "area_between_country","country_between_city","data_center","city_between_data_center","room","rack","rack_between_network","rack_between_server","rack_between_storage",\
            "pool","cluster","volume","vm"]
        db_mysql=Connect_Mysql(self.config1)
        cursor=db_mysql.client.cursor()
        for i in lt:
            cursor.execute(f"SELECT COUNT(*) AS count FROM {i}")
            result=cursor.fetchone()["count"]
            if result==0:
                flag=False
                break
        else:
            self.m.create_space(client,self.name)
            time.sleep(15)
        conn.close_nebula()
        db_mysql.close()
        return flag

    def run(self):
        self.create_area_nodes()
        self.create_country_nodes()
        self.create_city_nodes()
        self.create_data_center_nodes()
        self.create_room_nodes()
        self.create_rack_nodes()
        self.create_network_nodes()
        self.create_server_nodes()
        self.create_storage_nodes()
        self.create_area_between_country()
        self.create_country_between_city()
        self.create_city_between_data_center()
        self.create_data_center_between_room()
        self.create_room_between_rack()
        self.create_rack_between_network()
        self.create_rack_between_server()
        self.create_rack_between_storage()
        self.create_interface_nodes()
        self.create_nic_nodes()
        self.create_network_between_interface()
        self.create_interface_to_interface()
        self.create_server_between_nic()
        self.create_interface_between_nic()
        self.create_pool_node_and_storage_between_pool()
        self.create_cluster_node_and_pool_between_cluster()
        self.create_volume_node_and_cluster_between_volume()
        self.create_vm_node_and_volume_between_vm()

    def create_network_nodes(self):
        self.create_nodes("network",["hostname"])
        time.sleep(15)

    def create_server_nodes(self):
        self.create_nodes("server",["hostname"])
        time.sleep(15)

    def create_storage_nodes(self):
        self.create_nodes("storage",["hostname"])
        time.sleep(15)

    def create_interface_nodes(self):
        # 稍微还是有点问题
        self.create_nodes("interface",["hostname","name"],chunk_size=10000)
        time.sleep(15)

    def create_nic_nodes(self):
        # 稍微还是有点问题
        db_mysql=Connect_Mysql(self.config1)
        df_=db_mysql.get_table_data("interface_between_nic","SELECT hostname,device_ip,brand,nic,function FROM interface_between_nic")
        db_mysql.close()
        self.create_nodes("nic",["hostname","nic"],df=df_)
        time.sleep(15)

    def create_area_nodes(self):
        self.create_nodes("area",["area_name"])
        time.sleep(15)

    def create_country_nodes(self):
        db_mysql=Connect_Mysql(self.config1)
        df_=db_mysql.get_table_data("country","SELECT * FROM country")["country_name"].values.tolist()
        db_mysql.close()
        for i in range(len(df_)):
            df_[i]="country|"+df_[i]
        df_=pd.DataFrame(df_,columns=["country_name"])
        self.create_nodes("country",["country_name"],df=df_)
        time.sleep(15)

    def create_city_nodes(self):
        db_mysql=Connect_Mysql(self.config1)
        df_=db_mysql.get_table_data("city","SELECT * FROM city")["city_name"].values.tolist()
        db_mysql.close()
        for i in range(len(df_)):
            df_[i]="city|"+df_[i]
        df_=pd.DataFrame(df_,columns=["city_name"])
        self.create_nodes("city",["city_name"],df=df_)
        time.sleep(15)

    def create_data_center_nodes(self):
        self.create_nodes("data_center",["data_center_name"])
        time.sleep(15)

    def create_room_nodes(self):
        self.create_nodes("room",["room_name"])
        time.sleep(15)

    def create_rack_nodes(self):
        self.create_nodes("rack",["rack_name"])
        time.sleep(15)

    def create_nodes(self,table_name,key_name_list,df=None,chunk_size=1000):
        if df is None:
            db_mysql=Connect_Mysql(self.config1)
            df=db_mysql.get_table_data(table_name,f"select * from {table_name}")
            db_mysql.close()
        conn=Connect_Nebula(self.config2)
        conn.open_nebula()
        client=conn.client
        client.execute(f"USE {self.name}")
        self.m.create_tag(client,table_name,{key:"string" for key in df.columns.to_list() if key not in key_name_list})
        time.sleep(7.5)
        self.m.create_nodes(client,table_name,key_name_list,df,chunk_size)
        time.sleep(7.5)
        conn.close_nebula()
        logging.info(f"建立节点{table_name}成功了。")

    def create_network_between_interface(self):
        db_mysql=Connect_Mysql(self.config1)
        relationship=db_mysql.get_table_data("interface","select hostname,name from interface").values.tolist()
        networks=set(db_mysql.get_table_data("network","select hostname from network")["hostname"].values.tolist())
        db_mysql.close()
        relationship1=[];relationship2=[]
        for i in relationship:
            if i[0] not in networks:
                continue
            relationship1.append([i[0],i[0]+"|"+i[1],""])
            relationship2.append([i[0]+"|"+i[1],i[0],""])
        self.create_edges("network_to_interface",relationship1,10000)
        self.create_edges("interface_to_network",relationship2,10000)
        time.sleep(15)

    def create_interface_to_interface(self):
        db_mysql=Connect_Mysql(self.config1)
        hostnames=set(db_mysql.get_table_data("network","SELECT hostname FROM network")["hostname"].values.tolist())
        temp=db_mysql.get_table_data("interface_to_interaface",f"SELECT hostname,lldpLocPortId,lldpRemSysName,lldpRemPortId FROM interface_to_interface").values.tolist()
        db_mysql.close()
        lt=[]
        for i in temp:
            if i[2]==None or i[2]=="" or i[2]=="-" or i[2]=="--" or i[2]=="---" or i[2].lower()=="nan" or i[2].lower()=="null" or i[2].lower()=="none":
                continue
            if i[3]==None or i[3]=="" or i[3]=="-" or i[3]=="--" or i[3]=="---" or i[3].lower()=="nan" or i[3].lower()=="null" or i[2].lower()=="none":
                continue
            if i[0] not in hostnames or i[2] not in hostnames:
                continue
            lt.append([i[0]+"|"+i[1],i[2]+"|"+i[3]])
        relationship=[]
        def fc(i):
            conn=Connect_Nebula(self.config2)
            conn.open_nebula()
            client=conn.client
            client.execute(f"USE {self.name}")
            result1=client.execute(f"MATCH (v) WHERE id(v) == \"{i[0]}\" return v;").as_data_frame().values.tolist()
            result2=client.execute(f"MATCH (v) WHERE id(v) == \"{i[1]}\" return v;").as_data_frame().values.tolist()
            if result1==[] or result2==[]:
                logging.info(f"{i[0]:<50s}{i[1]:<50s}无法建立连接。")
            else:
                relationship.append([i[0],i[1],""])
                relationship.append([i[1],i[0],""])
            conn.close_nebula()
        with ThreadPoolExecutor(max_workers=100) as executor:
            pool=[]
            for i in lt:
                pool.append(executor.submit(fc,i))
            for task in as_completed(pool):
                task.result()
        self.create_edges("interface_to_interface",relationship)
        time.sleep(15)

    def create_server_between_nic(self):
        db_mysql=Connect_Mysql(self.config1)
        hostnames=set(db_mysql.get_table_data("server","SELECT hostname FROM server")["hostname"].values.tolist())
        relationship=db_mysql.get_table_data("interface_between_nic","SELECT hostname,nic FROM interface_between_nic").values.tolist()
        db_mysql.close()
        relationship1=[];relationship2=[]
        for i in relationship:
            if i[0] not in hostnames:
                continue
            relationship1.append([i[0],i[0]+"|"+i[1],""])
            relationship2.append([i[0]+"|"+i[1],i[0],""])
        self.create_edges("server_to_nic",relationship1)
        self.create_edges("nic_to_server",relationship2)
        time.sleep(15)

    def create_interface_between_nic(self):
        db_mysql=Connect_Mysql(self.config1)
        hostnames=set(db_mysql.get_table_data("network","SELECT hostname FROM network")["hostname"].values.tolist())
        temp=db_mysql.get_table_data("interface_between_nic","SELECT hostname,nic,sysname,port FROM interface_between_nic").values.tolist()
        db_mysql.close()
        lt=[]
        for i in temp:
            if i[2] not in hostnames:
                continue
            lt.append([i[0]+"|"+i[1],i[2]+"|"+i[3]])
        relationship1=[];relationship2=[]
        def fc(i):
            conn=Connect_Nebula(self.config2)
            conn.open_nebula()
            client=conn.client
            client.execute(f"USE {self.name}")
            result1=client.execute(f"MATCH (v) WHERE id(v) == \"{i[0]}\" return v;").as_data_frame().values.tolist()
            result2=client.execute(f"MATCH (v) WHERE id(v) == \"{i[1]}\" return v;").as_data_frame().values.tolist()
            if result1==[] or result2==[]:
                logging.info(f"{i[0]:<50s}{i[1]:<50s}无法建立连接。")
            else:
                relationship1.append([i[0],i[1],""])
                relationship2.append([i[1],i[0],""])
            conn.close_nebula()
        with ThreadPoolExecutor(max_workers=100) as executor:
            pool=[]
            for i in lt:
                pool.append(executor.submit(fc,i))
            for task in as_completed(pool):
                task.result()
        self.create_edges("nic_to_interface",relationship1)
        self.create_edges("interface_to_nic",relationship2)
        time.sleep(15)

    def create_area_between_country(self):
        db_mysql=Connect_Mysql(self.config1)
        temp=db_mysql.get_table_data("area_between_country","SELECT * FROM area_between_country").values.tolist()
        db_mysql.close()
        relationship1=[];relationship2=[]
        for i in temp:
            relationship1.append([i[0],"country|"+i[1],""])
            relationship2.append(["country|"+i[1],i[0],""])
        self.create_edges("area_to_country",relationship1)
        self.create_edges("country_to_area",relationship2)
        time.sleep(15)

    def create_country_between_city(self):
        db_mysql=Connect_Mysql(self.config1)
        temp=db_mysql.get_table_data("country_between_city","SELECT * FROM country_between_city").values.tolist()
        db_mysql.close()
        relationship1=[];relationship2=[]
        for i in temp:
            relationship1.append(["country|"+i[0],"city|"+i[1],""])
            relationship2.append(["city|"+i[1],"country|"+i[0],""])
        self.create_edges("country_to_city",relationship1)
        self.create_edges("city_to_country",relationship2)
        time.sleep(15)

    def create_city_between_data_center(self):
        db_mysql=Connect_Mysql(self.config1)
        temp=db_mysql.get_table_data("city_between_data_center","SELECT * FROM city_between_data_center").values.tolist()
        db_mysql.close()
        relationship1=[];relationship2=[]
        for i in temp:
            relationship1.append(["city|"+i[0],i[1],""])
            relationship2.append([i[1],"city|"+i[0],""])
        self.create_edges("city_to_data_center",relationship1)
        self.create_edges("data_center_to_city",relationship2)
        time.sleep(15)

    def create_data_center_between_room(self):
        db_mysql=Connect_Mysql(self.config1)
        temp=db_mysql.get_table_data("room","SELECT room_name FROM room")["room_name"].values.tolist()
        db_mysql.close()
        relationship1=[];relationship2=[]
        for _ in temp:
            i=_.split("|")[0];j=_
            relationship1.append([i,j,""])
            relationship2.append([j,i,""])
        self.create_edges("data_center_to_room",relationship1)
        self.create_edges("room_to_data_center",relationship2)
        time.sleep(15)

    def create_room_between_rack(self):
        db_mysql=Connect_Mysql(self.config1)
        temp=db_mysql.get_table_data("rack","SELECT rack_name FROM rack")["rack_name"].values.tolist()
        db_mysql.close()
        relationship1=[];relationship2=[]
        for _ in temp:
            s=_.split("|")
            i=s[0]+"|"+s[1];j=_
            relationship1.append([i,j,""])
            relationship2.append([j,i,""])
        self.create_edges("room_to_rack",relationship1)
        self.create_edges("rack_to_room",relationship2)
        time.sleep(15)

    def create_rack_between_network(self):
        db_mysql=Connect_Mysql(self.config1)
        temp=db_mysql.get_table_data("rack_between_network","SELECT * FROM rack_between_network").values.tolist()
        db_mysql.close()
        relationship1=[];relationship2=[]
        for i in temp:
            relationship1.append([i[0],i[1],""])
            relationship2.append([i[1],i[0],""])
        self.create_edges("rack_to_network",relationship1)
        self.create_edges("network_to_rack",relationship2)
        time.sleep(15)

    def create_rack_between_server(self):
        db_mysql=Connect_Mysql(self.config1)
        temp=db_mysql.get_table_data("rack_between_server","SELECT * FROM rack_between_server").values.tolist()
        db_mysql.close()
        relationship1=[];relationship2=[]
        for i in temp:
            relationship1.append([i[0],i[1],""])
            relationship2.append([i[1],i[0],""])
        self.create_edges("rack_to_server",relationship1)
        self.create_edges("server_to_rack",relationship2)
        time.sleep(15)

    def create_rack_between_storage(self):
        db_mysql=Connect_Mysql(self.config1)
        temp=db_mysql.get_table_data("rack_between_storage","SELECT * FROM rack_between_storage").values.tolist()
        db_mysql.close()
        relationship1=[];relationship2=[]
        for i in temp:
            relationship1.append([i[0],i[1],""])
            relationship2.append([i[1],i[0],""])
        self.create_edges("rack_to_storage",relationship1)
        self.create_edges("storage_to_rack",relationship2)
        time.sleep(15)

    def create_edges(self,edge_type,relationship,chunk_size=1000):
        conn=Connect_Nebula(self.config2)
        conn.open_nebula()
        client=conn.client
        client.execute(f"USE {self.name}")
        self.m.create_edge_type(client,edge_type)
        time.sleep(7.5)
        self.m.create_edges(client,edge_type,relationship,chunk_size)
        time.sleep(7.5)
        conn.close_nebula()

    def create_pool_node_and_storage_between_pool(self):
        db_mysql=Connect_Mysql(self.config1)
        pools=db_mysql.get_table_data("pool","select * from pool")["pool_name"].values.tolist()
        db_mysql=Connect_Mysql(self.config1)
        self.create_nodes("pool",["pool_name"])
        time.sleep(15)
        relationship1=[];relationship2=[]
        for pool in pools:
            temp=pool.split("|")
            relationship1.append([temp[0],pool,""])
            relationship2.append([pool,temp[0],""])
        self.create_edges("storage_to_pool",relationship1)
        self.create_edges("pool_to_storage",relationship2)
        time.sleep(15)

    def create_cluster_node_and_pool_between_cluster(self):
        db_mysql=Connect_Mysql(self.config1)
        clusters=db_mysql.get_table_data("cluster","select * from cluster")["cluster_name"].values.tolist()
        db_mysql.close()
        self.create_nodes("cluster",["cluster_name"])
        time.sleep(15)
        relationship1=[];relationship2=[]
        for cluster in clusters:
            temp=cluster.split("|")
            relationship1.append([temp[0]+"|"+temp[1],cluster,""])
            relationship2.append([cluster,temp[0]+"|"+temp[1],""])
        self.create_edges("pool_to_cluster",relationship1)
        self.create_edges("cluster_to_pool",relationship2)
        time.sleep(15)

    def create_volume_node_and_cluster_between_volume(self):
        db_mysql=Connect_Mysql(self.config1)
        volumes=db_mysql.get_table_data("volume","select * from volume")["volume_name"].values.tolist()
        db_mysql.close()
        self.create_nodes("volume",["volume_name"])
        time.sleep(15)
        relationship1=[];relationship2=[]
        for volume in volumes:
            temp=volume.split("|")
            relationship1.append([temp[0]+"|"+temp[1]+"|"+temp[2],volume,""])
            relationship2.append([volume,temp[0]+"|"+temp[1]+"|"+temp[2],""])
        self.create_edges("cluster_to_volume",relationship1)
        self.create_edges("volume_to_cluster",relationship2)
        time.sleep(15)

    def create_vm_node_and_volume_between_vm(self):
        db_mysql=Connect_Mysql(self.config1)
        vms=db_mysql.get_table_data("vm","select * from vm")["vm_name"].values.tolist()
        db_mysql.close()
        temp=pd.DataFrame([i.split("|")[-1] for i in vms],columns=["vm_name"])
        self.create_nodes("vm",["vm_name"],df=temp)
        time.sleep(15)
        relationship1=[];relationship2=[]
        for vm in vms:
            temp=vm.split("|")
            volume="|".join(temp[:-1]);vm=temp[-1]
            relationship1.append([volume,vm,""])
            relationship2.append([vm,volume,""])
        self.create_edges("volume_to_vm",relationship1)
        self.create_edges("vm_to_volume",relationship2)
        time.sleep(15)

if __name__=="__main__":
    config1={
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
    config2={
        "connection":{
            "TIMES":1000,
            "TIME":0.1
        },
        "nebula":{
            "HOST":"10.200.28.21",
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