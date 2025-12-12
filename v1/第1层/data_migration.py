import os
import logging
from logging.handlers import RotatingFileHandler
from connect import Connect_Mongodb,Connect_Mysql,Connect_Clickhouse
from bson import ObjectId
import pandas as pd
import datetime

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

class Data_Migration:

    def __init__(self,config1,config2,config3):
        self.db_mongo=Connect_Mongodb(config1)
        self.db_mysql=Connect_Mysql(config2)
        self.db_clickhouse=Connect_Clickhouse(config3)
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
                    'location.status': 1
                }
            },
            {
                '$project':None
            }
        ]
        self.cursor=self.db_mysql.client.cursor()

    def run(self):
        self.migrate_network()
        self.migrate_server()
        self.migrate_storage()
        self.migrate_interface()
        ##################################################
        self.migrate_area()
        self.migrate_country()
        self.migrate_city()
        self.migrate_area_between_country()
        self.migrate_country_between_city()
        self.migrate_data_center()
        self.migrate_city_between_data_center()
        self.migrate_room()
        self.migrate_rack()
        self.migrate_rack_between_network()
        self.migrate_rack_between_server()
        self.migrate_rack_between_storage()
        self.migrate_pod()
        ##################################################
        self.migrate_pool()
        self.migrate_cluster()
        self.migrate_volume()
        self.migrate_vm()

    def migrate_network(self):
        temp=self.pipeline.copy();temp[3]['$project']={"hostname":1,"sn_code":1,"patch_code":1,"device_ip":1,"device_ip_mask":1,"snmp_ip":1,"model":1,"system_version":1,"brand":1,"update_at":1}
        data=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_network.aggregate(temp))).astype(str)
        self.fc1(data,"network",["hostname"])
        logging.info("network迁移完成。")

    def migrate_server(self):
        temp=self.pipeline.copy();temp[3]['$project']={"hostname":1,"sn_code":1,"device_ip":1,"device_ip_mask":1,"out_band_ip":1,"out_band_ip_mask":1,"cluster_ip_1":1,"cluster_ip_2":1,"cluster_ip_2_mask":1,"storage_ip":1,"storage_ip_mask":1,"vmotion_ip_1":1,"vmotion_ip_1_mask":1,"vmotion_ip_2":1,"vmotion_ip_2_mask":1,"login_username":1,"login_password":1,"model":1,"system_version":1,"MemorySummary":1,"ProcessorSummary":1,"vc_host_memory":1,"vc_host_model":1,"vc_host_vendor":1,"brand":1,"update_at":1}
        data=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_server.aggregate(temp))).astype(str)
        data.rename(columns={"MemorySummary":"memorysummary","ProcessorSummary":"processorsummary"},inplace=True)
        self.fc1(data,"server",["hostname"])
        logging.info("server迁移完成。")

    def migrate_storage(self):
        temp=self.pipeline.copy();temp[3]['$project']={"hostname":1,"sn_code":1,"dev_code":1,"device_ip":1,"device_ip_mask":1,"out_band_ip":1,"out_band_ip_mask":1,"controller_ip_1":1,"controller_ip_1_mask":1,"controller_ip_2":1,"controller_ip_2_mask":1,"controller_ip_3":1,"controller_ip_3_mask":1,"system_version":1,"update_at":1}
        data=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_storage.aggregate(temp))).astype(str)
        self.fc1(data,"storage",["hostname"])
        logging.info("storage迁移完成。")

    def migrate_interface(self):
        # 第二层要控制一下
        data=self.db_mongo.get_collection("cds_ci_att_value_interface",{"status":1,"interface_status":{"$in":[ObjectId("5ecb7db73849236fc7315a1e"),ObjectId("5ecb7dbd3849236fc7315a20")]}},{"hostname":1,"name":1,"device_id":1,"high_speed":1,"hit_level_alert":1,"hit_level_critical":1,"update_at":1})
        self.fc1(data,"interface",["hostname","name"])
        logging.info("interface迁移完成。")

    def fc1(self,data,table_name,keys,default=True):
        if default:
            del data["_id"]
        columns=data.columns.to_list();columns_str=', '.join(columns)
        values_placeholders=', '.join(['%s']*len(columns))
        update_clause=', '.join([f"{column} = VALUES({column})" for column in columns if column not in keys])
        sql=f'''
            INSERT INTO {table_name} ({columns_str}) VALUES ({values_placeholders}) ON DUPLICATE KEY UPDATE {update_clause};
        '''
        values_list=[tuple(row) for row in data.values.tolist()]
        self.cursor.executemany(sql,values_list)
        self.db_mysql.client.commit()

    def migrate_area(self):
        dataset=self.db_mongo.get_collection("cds_ci_att_value_position",{"status":1},{"area":1})["area"].values.tolist()
        data=[]
        for i in dataset:
            if i=="" or i=="nan":
                continue
            temp=i[i.index("\'")+1:]
            temp=temp[:temp.index("\'")]
            data.append(temp)
        dict_=self.db_mongo.get_collection("cds_dict_detail",{"status":1},{"ui_name":1})
        dict_=dict(zip(dict_["_id"].values.tolist(),dict_["ui_name"].values.tolist()))
        data=list(set([dict_.get(i,None) for i in data]))
        self.fc2(data,"area")
        logging.info("area迁移完成。")

    def migrate_country(self):
        dataset=self.db_mongo.get_collection("cds_ci_att_value_position",{"status":1},{"country":1})["country"].values.tolist()
        data=list(set(dataset))
        self.fc2(data,"country")
        logging.info("country迁移完成。")

    def migrate_city(self):
        dataset=self.db_mongo.get_collection("cds_ci_att_value_position",{"status":1},{"city":1})["city"].values.tolist()
        data=list(set(dataset))
        self.fc2(data,"city")
        logging.info("city迁移完成。")

    def fc2(self,data,table_name):
        sql=f"INSERT INTO {table_name} ({table_name}_name) VALUES "
        if data:
            value_str=", ".join([f"(\'{i}\')" for i in data])
            sql=sql+value_str+";"
            self.cursor.execute(sql)
            self.db_mysql.client.commit()

    def migrate_area_between_country(self):
        dict_=self.db_mongo.get_collection("cds_dict_detail",{"status":1},{"ui_name":1})
        dict_=dict(zip(dict_["_id"].values.tolist(),dict_["ui_name"].values.tolist()))
        dataset=self.db_mongo.get_collection("cds_ci_att_value_position",{"status":1},{"area":1,"country":1}).values.tolist()
        data=[]
        for _ in dataset:
            i,j=_[2],_[1]
            if i=="" or i=="nan":
                continue
            temp=i[i.index("\'")+1:]
            temp=temp[:temp.index("\'")]
            data.append((dict_.get(temp,None),j))
        self.fc3("area_between_country",["area_name","country_name"],data)
        logging.info("area_between_country迁移完成。")

    def migrate_country_between_city(self):
        dataset=self.db_mongo.get_collection("cds_ci_att_value_position",{"status":1},{"country":1,"city":1}).values.tolist()
        data=[]
        for i in dataset:
            data.append((i[2],i[1]))
        self.fc3("country_between_city",["country_name","city_name"],data)
        logging.info("country_between_city迁移完成。")

    def fc3(self,table_name,columns,data):
        placeholders=", ".join(["%s"]*len(columns))
        update_clause=", ".join([f"{col}=VALUES({col})" for col in columns])
        sql=f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause};"
        self.cursor.executemany(sql,data)
        self.db_mysql.client.commit()

    def migrate_data_center(self):
        data=self.db_mongo.get_collection("cds_ci_att_value_data_center",{"status":1,"data_center_status":ObjectId("60f6670501572e47dad22c90")},{"data_center_name":1,"address":1,"latitude":1,"longitude":1,"nick_name":1,"type_name":1})
        self.fc1(data,"data_center",["data_center_name"])
        logging.info("data_center迁移完成。")

    def migrate_city_between_data_center(self):
        city_name_list=self.db_mysql.get_table_data("city","select city_name from city")["city_name"].values.tolist()
        data_center_name_list=self.db_mysql.get_table_data("data_center","select data_center_name from data_center")["data_center_name"].values.tolist()
        city=self.db_mongo.get_collection("cds_ci_att_value_position",{"status":1},{"_id":1,"city":1})
        city=dict(zip(city["_id"].values.tolist(),city["city"].values.tolist()))
        data_center=self.db_mongo.get_collection("cds_ci_att_value_data_center",{"status":1,"data_center_status":ObjectId("60f6670501572e47dad22c90")},{"_id":1,"data_center_name":1})
        data_center=dict(zip(data_center["_id"].values.tolist(),data_center["data_center_name"].values.tolist()))
        relationship=self.db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"data_center"},{"data_center_id":1,"position_id":1}).values.tolist()
        data=[]
        for i in relationship:
            x=None
            if city.get(i[1],None) in city_name_list:
                x=city.get(i[1],None)
            y=None
            if data_center.get(i[2],None) in data_center_name_list:
                y=data_center.get(i[2],None)
            if x and y:
                data.append((x,y))
        self.fc3("city_between_data_center",["city_name","data_center_name"],data)
        logging.info("city_between_data_center迁移完成。")

    def migrate_room(self):
        data_center=self.db_mongo.get_collection("cds_ci_att_value_data_center",{"status":1,"data_center_status":ObjectId("60f6670501572e47dad22c90")},{"_id":1,"data_center_name":1})
        data_center=dict(zip(data_center["_id"].values.tolist(),data_center["data_center_name"].values.tolist()))
        room=self.db_mongo.get_collection("cds_ci_att_value_room",{"status":1},{"_id":1,"room_name":1,"length":1,"width":1})
        room=dict(zip(room["_id"].values.tolist(),room[["room_name","length","width"]].values.tolist()))
        relationship=self.db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"room"},{"data_center_id":1,"room_id":1}).values.tolist()
        data=[]
        for i in relationship:
            x=None
            if data_center.get(i[1],None):
                x=data_center[i[1]]
            y=None
            if room.get(i[2],None):
                y=room[i[2]]
            if x and y:
                y[0]=x+"|"+y[0]
                data.append([y[0],y[1],y[2]])
        data=pd.DataFrame(data,columns=["room_name","length","width"])
        self.fc1(data,"room",["room_name"],False)
        logging.info("room迁移完成。")

    def migrate_rack(self):
        rack=self.db_mongo.get_collection("cds_ci_att_value_rack",{"status":1},{"data_center_name":1,"room_name":1,"rack_name":1,"rack_status":1,"rack_all_height":1,"rack_xy":1,"rack_up_down":1,"power_unit":1,"std_quantity":1,"add_power_quantity":1})
        room=self.db_mysql.get_table_data("room","select room_name from room")["room_name"].values.tolist()
        zd=self.db_mongo.get_collection("cds_dict_detail",{"status":1},{"_id":1,"field_name":1})
        zd=dict(zip(zd["_id"].values.tolist(),zd["field_name"].values.tolist()))
        def fc(s):
            try:
                s=s[s.index("\'")+1:]
                s=s[:s.index("\'")]
            except:
                s=""
            return s
        data=[]
        for i in range(rack.shape[0]):
            temp=[]
            if rack["data_center_name"][i]+"|"+rack["room_name"][i] not in room:
                continue
            temp.append(rack["data_center_name"][i]+"|"+rack["room_name"][i]+"|"+rack["rack_name"][i])
            temp.append(rack["rack_status"][i])
            temp.append(rack["rack_all_height"][i])
            temp.append(rack["rack_xy"][i])
            temp.append(zd.get(fc(rack["rack_up_down"][i]),None))
            temp.append(rack["power_unit"][i])
            temp.append(rack["std_quantity"][i])
            temp.append(rack["add_power_quantity"][i])
            data.append(temp)
        data=pd.DataFrame(data,columns=["rack_name","rack_status","rack_all_height","rack_xy","rack_up_or_down","power_unit","std_quantity","add_power_quantity"])
        self.fc1(data,"rack",["rack_name"],False)  
        logging.info("rack迁移完成。") 

    def migrate_rack_between_network(self):
        relationship=self.db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"network"},{"rack_id":1,"network_id":1})[["rack_id","network_id"]].values.tolist()
        rack=self.db_mongo.get_collection("cds_ci_att_value_rack",{"status":1},{"_id":1,"data_center_name":1,"room_name":1,"rack_name":1})
        rack_zd=dict(zip(rack["_id"].values.tolist(),["|".join([rack["data_center_name"][i],rack["room_name"][i],rack["rack_name"][i]]) for i in range(rack.shape[0])]))
        rack_name_list=self.db_mysql.get_table_data("rack","select rack_name from rack")["rack_name"].values.tolist()
        temp=self.pipeline.copy();temp[3]['$project']={"_id":1,"hostname":1}
        network=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_network.aggregate(temp))).astype(str)
        network_zd=dict(zip(network["_id"].values.tolist(),network["hostname"].values.tolist()))
        network_name_list=self.db_mysql.get_table_data("network","select hostname from network")["hostname"].values.tolist()
        data=[]
        for i in relationship:
            x=None
            if rack_zd.get(i[0],None) in rack_name_list:
                x=rack_zd[i[0]]
            y=None
            if network_zd.get(i[1],None) in network_name_list:
                y=network_zd[i[1]]
            if x and y:
                data.append((x,y))
        self.fc3("rack_between_network",["rack_name","hostname"],data)
        logging.info("rack_between_network迁移完成。")
    
    def migrate_rack_between_server(self):
        relationship=self.db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"server"},{"rack_id":1,"server_id":1})[["rack_id","server_id"]].values.tolist()
        rack=self.db_mongo.get_collection("cds_ci_att_value_rack",{"status":1},{"_id":1,"data_center_name":1,"room_name":1,"rack_name":1})
        rack_zd=dict(zip(rack["_id"].values.tolist(),["|".join([rack["data_center_name"][i],rack["room_name"][i],rack["rack_name"][i]]) for i in range(rack.shape[0])]))
        rack_name_list=self.db_mysql.get_table_data("rack","select rack_name from rack")["rack_name"].values.tolist()
        temp=self.pipeline.copy();temp[3]['$project']={"_id":1,"hostname":1}
        server=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_server.aggregate(temp))).astype(str)
        server_zd=dict(zip(server["_id"].values.tolist(),server["hostname"].values.tolist()))
        server_name_list=self.db_mysql.get_table_data("server","select hostname from server")["hostname"].values.tolist()
        data=[]
        for i in relationship:
            x=None
            if rack_zd.get(i[0],None) in rack_name_list:
                x=rack_zd[i[0]]
            y=None
            if server_zd.get(i[1],None) in server_name_list:
                y=server_zd[i[1]]
            if x and y:
                data.append((x,y))
        self.fc3("rack_between_server",["rack_name","hostname"],data)
        logging.info("rack_between_server迁移完成。")
    
    def migrate_rack_between_storage(self):
        relationship=self.db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"storage"},{"rack_id":1,"storage_id":1})[["rack_id","storage_id"]].values.tolist()
        rack=self.db_mongo.get_collection("cds_ci_att_value_rack",{"status":1},{"_id":1,"data_center_name":1,"room_name":1,"rack_name":1})
        rack_zd=dict(zip(rack["_id"].values.tolist(),["|".join([rack["data_center_name"][i],rack["room_name"][i],rack["rack_name"][i]]) for i in range(rack.shape[0])]))
        rack_name_list=self.db_mysql.get_table_data("rack","select rack_name from rack")["rack_name"].values.tolist()
        temp=self.pipeline.copy();temp[3]['$project']={"_id":1,"hostname":1}
        storage=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_storage.aggregate(temp))).astype(str)
        storage_zd=dict(zip(storage["_id"].values.tolist(),storage["hostname"].values.tolist()))
        storage_name_list=self.db_mysql.get_table_data("storage","select hostname from storage")["hostname"].values.tolist()
        data=[]
        for i in relationship:
            x=None
            if rack_zd.get(i[0],None) in rack_name_list:
                x=rack_zd[i[0]]
            y=None
            if storage_zd.get(i[1],None) in storage_name_list:
                y=storage_zd[i[1]]
            if x and y:
                data.append((x,y))
        self.fc3("rack_between_storage",["rack_name","hostname"],data)
        logging.info("rack_between_storage迁移完成。")

    def migrate_pool(self):
        storage=self.db_mysql.get_table_data("storage","select * from storage")["hostname"].values.tolist()
        date_time=datetime.datetime.now()-datetime.timedelta(days=1)
        jh=set()
        for i in storage:
            query="select scaleio,pool from ods_vm.vc_volume_info_all  where toDateTime(monitor_time)  > '{}:00' and scaleio='{}' group by scaleio,pool;".format(str(date_time)[:16],i)
            temp=self.db_clickhouse.query(query).values.tolist()
            for j in temp:
                if j[1].strip()=="":
                    continue
                jh.add(j[0]+"|"+j[1])
        self.fc2(list(jh),"pool")
        logging.info("pool迁移完成。")

    def migrate_cluster(self):
        pools=self.db_mysql.get_table_data("pool","select * from pool")["pool_name"].values.tolist()
        date_time=datetime.datetime.now()-datetime.timedelta(days=1)
        jh=set()
        for pool in pools:
            pool=pool.split("|")
            x=pool[0];y=pool[1]
            query="select name from ods_vm.vc_volume_info_all  where toDateTime(monitor_time)  > '{}:00' and scaleio='{}' and pool='{}' group by name;".format(str(date_time)[:16],x,y)
            temp=self.db_clickhouse.query(query)["name"].values.tolist()
            for cluster in temp:
                cluster=cluster.split("-")
                jh.add(x+"|"+y+"|"+cluster[0]+"-"+cluster[1])
        self.fc2(list(jh),"cluster")
        logging.info("cluster迁移完成。")

    def migrate_volume(self):
        pools=self.db_mysql.get_table_data("pool","select * from pool")["pool_name"].values.tolist()
        date_time=datetime.datetime.now()-datetime.timedelta(days=1)
        jh=set()
        for pool in pools:
            pool=pool.split("|")
            x=pool[0];y=pool[1]
            query="select name from ods_vm.vc_volume_info_all  where toDateTime(monitor_time)  > '{}:00' and scaleio='{}' and pool='{}' group by name;".format(str(date_time)[:16],x,y)
            volumes=self.db_clickhouse.query(query)["name"].values.tolist()
            for volume in volumes:
                temp=volume.split("-")
                jh.add(x+"|"+y+"|"+temp[0]+"-"+temp[1]+"|"+volume)
        self.fc2(list(jh),"volume")
        logging.info("volume迁移完成。")

    def migrate_vm(self):
        volumes=self.db_mysql.get_table_data("volume","select * from volume")["volume_name"].values.tolist()
        date_time=datetime.datetime.now()-datetime.timedelta(days=1)
        jh=set()
        for volume in volumes:
            volume_temp=volume.split("|")[-1]
            query="select vm from ods_vm.vc_volume_info_all  where toDateTime(monitor_time)  > '{}:00' and name='{}' group by vm;".format(str(date_time)[:16],volume_temp)
            vms=self.db_clickhouse.query(query)["vm"].values.tolist()[0].split(",")
            for vm in vms:
                if vm.strip()=="":
                    continue
                jh.add(volume+"|"+vm)
        self.fc2(list(jh),"vm")
        logging.info("vm迁移完成。")

    def migrate_pod(self):
        temp=[
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
                '$project':{
                    "code":1
                }
            }
        ]
        pods=set(pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_pod.aggregate(temp))).astype(str)["code"].values.tolist())
        data_centers=set(self.db_mysql.get_table_data("data_center","select data_center_name from data_center")["data_center_name"].values.tolist())
        relationships=self.db_mongo.get_collection("cds_ci_relation_detail",{"cds_ci_relation_id":ObjectId("5ec8c70b94285cfd9cacef1f"),"status":1},{"cds_ci_value_destination_id":1,"cds_ci_value_source_id":1})[["cds_ci_value_destination_id","cds_ci_value_source_id"]].values.tolist()
        pods_zd=self.db_mongo.get_collection("cds_ci_att_value_pod",{"status":1},{"_id":1,"code":1})
        data_centers_zd=self.db_mongo.get_collection("cds_ci_att_value_data_center",{"status":1},{"_id":1,"data_center_name":1})
        pods_zd=dict(zip(pods_zd["_id"].values.tolist(),pods_zd["code"].values.tolist()))
        data_centers_zd=dict(zip(data_centers_zd["_id"].values.tolist(),data_centers_zd["data_center_name"].values.tolist()))
        temp=[]
        for relationship in relationships:
            i,j=relationship[0],relationship[1]
            if pods_zd.get(i,None) in pods and data_centers_zd.get(j,None) in data_centers:
                temp.append([pods_zd[i],data_centers_zd[j]])
        data=pd.DataFrame(temp,columns=["pod_name","data_center_name"])
        self.fc1(data,"pod",["pod_name"],default=False)
        logging.info("pod迁移完成。")

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
    config3={
        "connection":{
            "TIMES":1000,
            "TIME":0.1
        },
        "clickhouse":{
            "HOST":"10.216.140.107",
            "PORT":9000,
            "USERNAME":"default",
            "PASSWORD":""
        }
    }
    m=Data_Migration(config1,config2,config3)
    m.run()