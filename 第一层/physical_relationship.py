from connect import Connect_Mysql,Connect_Mongodb
from bson import ObjectId
import pandas as pd

class Run:

    def __init__(self,config1,config2):
        self.config1=config1
        self.config2=config2
        self.db_mysql=Connect_Mysql(config1)
        self.db_mysql_client=self.db_mysql.client.cursor()
        self.db_mongo=Connect_Mongodb(config2)
        self.db_mongo_client=self.db_mongo.client
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
            }
        ]

    def create_area_node(self):
        sql='''
        create table if not exists topu.area (
            area_name VARCHAR(100) primary key
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.area;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        temp=list(set(self.db_mongo.get_collection("cds_ci_att_value_position",{"status":1},{"area":1})["area"].values.tolist()))
        area=[]
        for i in temp:
            try:
                i=i[i.index("'")+1:]
                i=i[:i.index("'")]
                area.append(i)
            except:
                pass
        zd=self.db_mongo.get_collection("cds_dict_detail",{"status":1},{"_id":1,"field_name":1})
        zd=dict(zip(zd["_id"].values.tolist(),zd["field_name"].values.tolist()))
        area=[zd[i]+"|area" for i in area]
        sql='''
        insert into topu.area (area_name) values (%s);
        '''
        self.db_mysql_client.executemany(sql,area)
        self.db_mysql.client.commit()

    def create_country_node(self):
        sql='''
        create table if not exists topu.country (
            country_name VARCHAR(100) primary key
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.country;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        country=list(set(self.db_mongo.get_collection("cds_ci_att_value_position",{"status":1},{"country":1})["country"].values.tolist()))
        country=[i+"|country" for i in country]
        sql='''
        insert into topu.country (country_name) values (%s);
        '''
        self.db_mysql_client.executemany(sql,country)
        self.db_mysql.client.commit()

    def create_edge_between_area_and_country(self):
        sql='''
        create table if not exists topu.between_area_and_country (
            area_name VARCHAR(100),
            country_name VARCHAR(100),
            primary key (area_name,country_name)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.between_area_and_country;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        area=set(self.db_mysql.get_table_data("","select area_name from topu.area")["area_name"].values.tolist())
        country=set(self.db_mysql.get_table_data("","select country_name from topu.country")["country_name"].values.tolist())
        temp=self.db_mongo.get_collection("cds_ci_att_value_position",{"status":1},{"area":1,"country":1})[["area","country"]].values.tolist()
        zd=self.db_mongo.get_collection("cds_dict_detail",{"status":1},{"_id":1,"field_name":1})
        zd=dict(zip(zd["_id"].values.tolist(),zd["field_name"].values.tolist()))
        relationship=[]
        for i in temp:
            x=i[0];y=i[1]
            try:
                x=x[x.index("'")+1:]
                x=x[:x.index("'")]
                x=zd[x]
                x+="|area"
                y+="|country"
                if x not in area or y not in country:
                    continue
                relationship.append((x,y))
            except:
                continue
        relationship=list(set(relationship))
        sql='''
        insert into topu.between_area_and_country (area_name,country_name) values (%s,%s);
        '''
        self.db_mysql_client.executemany(sql,relationship)
        self.db_mysql.client.commit()

    def create_city_node(self):
        sql='''
        create table if not exists topu.city (
            city_name VARCHAR(100) primary key,
            code VARCHAR(25)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.city;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        city=list(set([(i[0]+"|city",i[1]) for i in self.db_mongo.get_collection("cds_ci_att_value_position",{"status":1},{"city":1,"code":1})[["city","code"]].values.tolist()]))
        sql='''
        insert into topu.city (city_name,code) values (%s,%s);
        '''
        self.db_mysql_client.executemany(sql,city)
        self.db_mysql.client.commit()

    def create_edge_between_country_and_city(self):
        sql='''
        create table if not exists topu.between_country_and_city (
            country_name VARCHAR(100),
            city_name VARCHAR(100),
            primary key (country_name,city_name)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.between_country_and_city;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        country=set(self.db_mysql.get_table_data("","select country_name from topu.country")["country_name"].values.tolist())
        city=set(self.db_mysql.get_table_data("","select city_name from topu.city")["city_name"].values.tolist())
        temp=self.db_mongo.get_collection("cds_ci_att_value_position",{"status":1},{"country":1,"city":1})[["country","city"]].values.tolist()
        relationship=[]
        for i in temp:
            x=i[0];y=i[1]
            x+="|country";y+="|city"
            if x not in country or y not in city:
                continue
            relationship.append((x,y))
        sql='''
        insert into topu.between_country_and_city (country_name,city_name) values (%s,%s);
        '''
        self.db_mysql_client.executemany(sql,relationship)
        self.db_mysql.client.commit()

    def create_data_center_node(self):
        sql='''
        create table if not exists topu.data_center (
            data_center_name VARCHAR(100) primary key,
            code VARCHAR(25),
            address text,
            longitude VARCHAR(100),
            latitude VARCHAR(100),
            type_name VARCHAR(25)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.data_center;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        data_center=self.db_mongo.get_collection("cds_ci_att_value_data_center",{"status":1,"data_center_status":{"$ne":ObjectId("60f66712e61a21f5aafd564a")}},{"data_center_name":1,"code":1,"address":1,"longitude":1,"latitude":1,"type_name":1})[["data_center_name","code","address","longitude","latitude","type_name"]].values.tolist()
        for i in range(len(data_center)):
            data_center[i][0]+="|data_center"
            data_center[i]=tuple(data_center[i])
        sql='''
        insert into topu.data_center (data_center_name,code,address,longitude,latitude,type_name) values (%s,%s,%s,%s,%s,%s);
        '''
        self.db_mysql_client.executemany(sql,data_center)
        self.db_mysql.client.commit()

    def create_edge_between_city_and_data_center(self):
        sql='''
        create table if not exists topu.between_city_and_data_center (
            city_name VARCHAR(100),
            data_center_name VARCHAR(100),
            primary key (city_name,data_center_name)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.between_city_and_data_center;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        zd=self.db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"data_center"},{"data_center_id":1,"position_id":1})[["position_id","data_center_id"]].values.tolist()
        jh1=set(self.db_mysql.get_table_data("","select city_name from topu.city")["city_name"].values.tolist())
        jh2=set(self.db_mysql.get_table_data("","select data_center_name from topu.data_center")["data_center_name"].values.tolist())
        zd1=self.db_mongo.get_collection("cds_ci_att_value_position",{"status":1},{"_id":1,"city":1})[["_id","city"]]
        zd1=dict(zip(zd1["_id"].values.tolist(),zd1["city"].values.tolist()))
        zd2=self.db_mongo.get_collection("cds_ci_att_value_data_center",{"status":1,"data_center_status":{"$ne":ObjectId("60f66712e61a21f5aafd564a")}},{"_id":1,"data_center_name":1})[["_id","data_center_name"]]
        zd2=dict(zip(zd2["_id"].values.tolist(),zd2["data_center_name"].values.tolist()))
        relationship=[]
        for i in zd:
            x=i[0];y=i[1]
            x=zd1.get(x,None);y=zd2.get(y,None)
            if not (x and y):
                continue
            x+="|city";y+="|data_center"
            if x not in jh1 or y not in jh2:
                continue
            relationship.append((x,y))
        sql='''
        insert into topu.between_city_and_data_center (city_name,data_center_name) values (%s,%s);
        '''
        self.db_mysql_client.executemany(sql,relationship)
        self.db_mysql.client.commit()

    def create_room_node(self):
        sql='''
        create table if not exists topu.room (
            room_name VARCHAR(100) primary key,
            code VARCHAR(100),
            length VARCHAR(25),
            width VARCHAR(25)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.room;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        temp=self.db_mongo.get_collection("cds_ci_att_value_room",{"status":1},{"room_name":1,"code":1,"length":1,"width":1,"data_center_name":1,"update_at":1})[["room_name","code","length","width","data_center_name","update_at"]].values.tolist()
        room={}
        for i in temp:
            room_name=i[-2]+"|"+i[0]+"|"+"room";code=i[1];length=i[2];width=i[3];time_=i[-1]
            if room_name not in room:
                room[room_name]=[room_name,code,length,width,time_]
            elif room[room_name][-1]<=time_:
                room[room_name]=[room_name,code,length,width,time_]
        room=[tuple(room[i][:-1]) for i in room]
        sql='''
        insert into topu.room (room_name,code,length,width) values (%s,%s,%s,%s);
        '''
        self.db_mysql_client.executemany(sql,room)
        self.db_mysql.client.commit()

    def create_edge_between_data_center_and_room(self):
        sql='''
        create table if not exists topu.between_data_center_and_room (
            data_center_name VARCHAR(100),
            room_name VARCHAR(100),
            primary key (data_center_name,room_name)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.between_data_center_and_room;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        zd=self.db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"room"},{"data_center_id":1,"room_id":1})[["data_center_id","room_id"]].values.tolist()
        jh1=set(self.db_mysql.get_table_data("","select data_center_name from topu.data_center")["data_center_name"].values.tolist())
        jh2=set(self.db_mysql.get_table_data("","select room_name from topu.room")["room_name"].values.tolist())
        zd1=self.db_mongo.get_collection("cds_ci_att_value_data_center",{"status":1,"data_center_status":{"$ne":ObjectId("60f66712e61a21f5aafd564a")}},{"_id":1,"data_center_name":1})[["_id","data_center_name"]]
        zd1=dict(zip(zd1["_id"].values.tolist(),zd1["data_center_name"].values.tolist()))
        zd2=self.db_mongo.get_collection("cds_ci_att_value_room",{"status":1},{"_id":1,"room_name":1,"data_center_name":1,"update_at":1})[["_id","room_name","data_center_name","update_at"]]
        zd2=dict(zip(zd2["_id"].values.tolist(),[(i[1]+"|"+i[0],i[2]) for i in zd2[["room_name","data_center_name","update_at"]].values.tolist()]))
        relationship={}
        for i in zd:
            x=i[0];y=i[1]
            x=zd1.get(x,None);y=zd2.get(y,None)
            if not (x and y):
                continue
            data_center_name=x+"|data_center";room_name=y[0]+"|room";time_=y[1]
            if data_center_name not in jh1 or room_name not in jh2:
                continue
            if room_name not in relationship:
                relationship[room_name]=[data_center_name,room_name,time_]
            elif relationship[room_name][-1]<=time_:
                relationship[room_name]=[data_center_name,room_name,time_]
        relationship=list(tuple(i[:-1]) for i in relationship.values())
        sql='''
        insert into topu.between_data_center_and_room (data_center_name,room_name) values (%s,%s);
        '''
        self.db_mysql_client.executemany(sql,relationship)
        self.db_mysql.client.commit()

    def create_rack_node(self):
        sql='''
        create table if not exists topu.rack (
            rack_name VARCHAR(100) primary key,
            rack_x VARCHAR(25),
            rack_y VARCHAR(25),
            rack_h VARCHAR(25),
            rack_direction VARCHAR(25)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.rack;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        temp=self.db_mongo.get_collection("cds_ci_att_value_rack",{"status":1},{"data_center_name":1,"room_name":1,"rack_name":1,"rack_xy":1,"rack_all_height":1,"rack_up_down":1,"update_at":1})[["data_center_name","room_name","rack_name","rack_xy","rack_all_height","rack_up_down","update_at"]].values.tolist()
        zd=self.db_mongo.get_collection("cds_dict_detail",{"status":1},{"_id":1,"field_name":1})
        zd=dict(zip(zd["_id"].values.tolist(),zd["field_name"].values.tolist()))
        rack={}
        for i in temp:
            rack_name=i[0]+"|"+i[1]+"|"+i[2]+"|"+"rack"
            if type(i[3])!=str or i[3].strip()=="":
                rack_x="";rack_y=""
            else:
                rack_x,rack_y=i[3].split(",")
            rack_h=i[4].strip()
            try:
                i[5]=i[5][i[5].index("'")+1:]
                i[5]=i[5][:i[5].index("'")]
            except:
                i[5]=i[5].strip()
            if rack_name not in rack:
                rack[rack_name]=[rack_name,rack_x,rack_y,rack_h,zd.get(i[5],""),i[-1]]
            elif rack[rack_name][-1]<=i[-1]:
                rack[rack_name]=[rack_name,rack_x,rack_y,rack_h,zd.get(i[5],""),i[-1]]
        rack=[tuple(i[:-1]) for i in rack.values()]
        sql='''
        insert into topu.rack (rack_name,rack_x,rack_y,rack_h,rack_direction) values (%s,%s,%s,%s,%s);
        '''
        self.db_mysql_client.executemany(sql,rack)
        self.db_mysql.client.commit()

    def create_edge_between_room_and_rack(self):
        sql='''
        create table if not exists topu.between_room_and_rack (
            room_name VARCHAR(100),
            rack_name VARCHAR(100),
            primary key (room_name,rack_name)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.between_room_and_rack;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        zd=self.db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"rack"},{"room_id":1,"rack_id":1})[["room_id","rack_id"]].values.tolist()
        jh1=set(self.db_mysql.get_table_data("","select room_name from topu.room")["room_name"].values.tolist())
        jh2=set(self.db_mysql.get_table_data("","select rack_name from topu.rack")["rack_name"].values.tolist())
        zd1=self.db_mongo.get_collection("cds_ci_att_value_room",{"status":1},{"_id":1,"data_center_name":1,"room_name":1})
        zd2=self.db_mongo.get_collection("cds_ci_att_value_rack",{"status":1},{"_id":1,"data_center_name":1,"room_name":1,"rack_name":1})
        zd1=dict(zip(zd1["_id"].values.tolist(),[i[0]+"|"+i[1]+"|room" for i in zd1[["data_center_name","room_name"]].values.tolist()]))
        zd2=dict(zip(zd2["_id"].values.tolist(),[i[0]+"|"+i[1]+"|"+i[2]+"|rack" for i in zd2[["data_center_name","room_name","rack_name"]].values.tolist()]))
        relationship=[]
        for i in zd:
            x=zd1.get(i[0],None);y=zd2.get(i[1],None)
            if not (x and y):
                continue
            if x not in jh1 or y not in jh2:
                continue
            relationship.append((x,y))
        relationship=list(set(relationship))
        sql='''
        insert into topu.between_room_and_rack (room_name,rack_name) values (%s,%s);
        '''
        self.db_mysql_client.executemany(sql,relationship)
        self.db_mysql.client.commit()

    def create_network_node(self):
        sql='''
        create table if not exists topu.network (
            hostname VARCHAR(100) primary key,
            ip VARCHAR(25),
            brand VARCHAR(25),
            u_begin VARCHAR(25),
            u_height VARCHAR(25),
            u_front_back VARCHAR(25),
            u_size VARCHAR(25)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.network;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        pipeline_copy=self.pipeline.copy()
        pipeline_copy.append(
            {
                '$project':{
                    "hostname":1,
                    "device_ip":1,
                    "brand":1,
                    "u_begin":1,
                    "u_height":1,
                    "u_front_back":1,
                    "u_size":1,
                    "update_at":1
                }
            }
        )
        network=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_network.aggregate(pipeline_copy))).astype(str)[["hostname","device_ip","brand","u_begin","u_height","u_front_back","u_size","update_at"]].values.tolist()
        zd=self.db_mongo.get_collection("cds_dict_detail",{"status":1},{"_id","field_name"})[["_id","field_name"]]
        zd=dict(zip(zd["_id"].values.tolist(),zd["field_name"].values.tolist()))
        for i in range(len(network)):
            hostname=network[i][0]
            hostname="-".join([i.strip() for i in hostname.split("-")])
            network[i][0]=hostname
            if "." not in network[i][1] or "-" in network[i][1] or network[i][1].count(".")>=4:
                network[i][1]=""
            else:
                ip=network[i][1]
                ip=".".join([i.strip() for i in ip.split(".")])
                network[i][1]=ip
            network[i][2]=network[i][2].lower()
            for j in range(3,7):
                network[i][j]=network[i][j].lower()
                if network[i][j] in ["nan","null","none","-","--","---"]:
                    network[i][j]=""
            x=network[i][-3]
            try:
                x=x[x.index("'")+1:]
                x=x[:x.index("'")]
                x=zd.get(x,"")
            except:
                x=""
            network[i][-3]=x
            y=network[i][-2]
            try:
                y=y[y.index("'")+1:]
                y=y[:y.index("'")]
                y=zd.get(y,"")
            except:
                y=""
            network[i][-2]=y
        temp={}
        for i in network:
            if i[0] not in temp:
                temp[i[0]]=i
            elif temp[i[0]][-1]<=i[-1]:
                temp[i[0]]=i
        network=[tuple(i[:-1]) for i in temp.values()]
        sql='''
        insert into topu.network (hostname,ip,brand,u_begin,u_height,u_front_back,u_size) values (%s,%s,%s,%s,%s,%s,%s)
        '''
        self.db_mysql_client.executemany(sql,network)
        self.db_mysql.client.commit()

    def create_edge_between_rack_and_network(self):
        sql='''
        create table if not exists topu.between_rack_and_network (
            rack_name VARCHAR(100),
            hostname VARCHAR(100),
            primary key (rack_name,hostname)
        )
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.between_rack_and_network;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        zd=self.db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"network"},{"rack_id":1,"network_id":1})[["rack_id","network_id"]].values.tolist()
        jh1=set(self.db_mysql.get_table_data("","select rack_name from topu.rack")["rack_name"].values.tolist())
        jh2=set(self.db_mysql.get_table_data("","select hostname from topu.network")["hostname"].values.tolist())
        zd1=self.db_mongo.get_collection("cds_ci_att_value_rack",{"status":1},{"_id":1,"data_center_name":1,"room_name":1,"rack_name":1})
        zd1=dict(zip(zd1["_id"].values.tolist(),[i[0]+"|"+i[1]+"|"+i[2]+"|rack" for i in zd1[["data_center_name","room_name","rack_name"]].values.tolist()]))
        pipeline_copy=self.pipeline.copy()
        pipeline_copy.append(
            {
                '$project':{
                    "_id":1,
                    "hostname":1
                }
            }
        )
        zd2=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_network.aggregate(pipeline_copy))).astype(str)
        zd2=dict(zip(zd2["_id"].values.tolist(),zd2["hostname"].values.tolist()))
        relationship=[]
        for i in zd:
            x=zd1.get(i[0],None);y=zd2.get(i[1],None)
            if not (x and y):
                continue
            if x not in jh1 or y not in jh2:
                continue
            relationship.append((x,y))
        sql='''
        insert into topu.between_rack_and_network (rack_name,hostname) values (%s,%s);
        '''
        self.db_mysql_client.executemany(sql,relationship)
        self.db_mysql.client.commit()

    def create_server_node(self):
        sql='''
        create table if not exists topu.server (
            hostname VARCHAR(100) primary key,
            in_band_ip VARCHAR(25),
            out_band_ip VARCHAR(25),
            out_band_ip_is_active VARCHAR(5),
            brand1 VARCHAR(25),
            brand2 VARCHAR(25),
            server_group VARCHAR(25),
            server_type VARCHAR(25),
            u_begin VARCHAR(25),
            u_height VARCHAR(25),
            u_front_back VARCHAR(25),
            u_size VARCHAR(25)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.server;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        pipeline_copy=self.pipeline.copy()
        pipeline_copy.append(
            {
                '$project':{
                    "hostname":1,
                    "device_ip":1,
                    "out_band_ip":1,
                    "out_band_ip_active":1,
                    "brand":1,
                    "web_brand":1,
                    "device_server_group":1,
                    "device_server_type":1,
                    "u_begin":1,
                    "u_height":1,
                    "u_front_back":1,
                    "u_size":1,
                    "update_at":1
                }
            }
        )
        temp=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_server.aggregate(pipeline_copy))).astype(str)[["hostname","device_ip","out_band_ip","out_band_ip_active","brand","web_brand","device_server_group","device_server_type","u_begin","u_height","u_front_back","u_size","update_at"]].values.tolist()
        zd=self.db_mongo.get_collection("cds_dict_detail",{"status":1},{"_id":1,"field_name":1})
        zd=dict(zip(zd["_id"].values.tolist(),zd["field_name"].values.tolist()))
        server={}
        for i in range(len(temp)):
            hostname=temp[i][0]
            hostname="-".join([i.strip() for i in hostname.split("-")])
            temp[i][0]=hostname
            in_band_ip=temp[i][1]
            if "." not in in_band_ip or "-" in in_band_ip or in_band_ip.count(".")>=4:
                in_band_ip=""
            else:
                in_band_ip=".".join([i.strip() for i in in_band_ip.split(".")])
            temp[i][1]=in_band_ip
            out_band_ip=temp[i][2]
            if "." not in out_band_ip or "-" in out_band_ip or out_band_ip.count(".")>=4:
                out_band_ip=""
            else:
                out_band_ip=".".join([i.strip() for i in out_band_ip.split(".")])
            temp[i][2]=out_band_ip
            out_band_ip_is_active=temp[i][3].strip()
            if out_band_ip_is_active not in ["0","1"]:
                out_band_ip_is_active=""
            temp[i][3]=out_band_ip_is_active
            temp[i][4]=temp[i][4].strip().lower()
            temp[i][5]=temp[i][5].strip().lower()
            for j in [6,7,10,11]:
                try:
                    x=temp[i][j]
                    x=x[x.index("'")+1:]
                    x=x[:x.index("'")]
                    x=zd.get(x,"")
                except:
                    x=""
                temp[i][j]=x
            if temp[i][0] not in server:
                server[temp[i][0]]=temp[i]
            elif server[temp[i][0]][-1]<=temp[i][-1]:
                server[temp[i][0]]=temp[i]
        server=[tuple(i[:-1]) for i in server.values()]
        sql='''
        insert into topu.server (hostname,in_band_ip,out_band_ip,out_band_ip_is_active,brand1,brand2,server_group,server_type,u_begin,u_height,u_front_back,u_size) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        self.db_mysql_client.executemany(sql,server)
        self.db_mysql.client.commit()

    def create_edge_between_rack_and_server(self):
        sql='''
        create table if not exists topu.between_rack_and_server (
            rack_name VARCHAR(100),
            hostname VARCHAR(100),
            primary key (rack_name,hostname)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.between_rack_and_server;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        zd=self.db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"server"},{"rack_id":1,"server_id":1})[["rack_id","server_id"]].values.tolist()
        jh1=set(self.db_mysql.get_table_data("","select rack_name from topu.rack")["rack_name"].values.tolist())
        jh2=set(self.db_mysql.get_table_data("","select hostname from topu.server")["hostname"].values.tolist())
        zd1=self.db_mongo.get_collection("cds_ci_att_value_rack",{"status":1},{"_id":1,"data_center_name":1,"room_name":1,"rack_name":1})
        pipeline_copy=self.pipeline.copy()
        pipeline_copy.append(
            {
                '$project':{
                    "_id":1,
                    "hostname":1
                }
            }
        )
        zd2=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_server.aggregate(pipeline_copy))).astype(str)
        zd1=dict(zip(zd1["_id"].values.tolist(),[i[0]+"|"+i[1]+"|"+i[2]+"|rack" for i in zd1[["data_center_name","room_name","rack_name"]].values.tolist()]))
        zd2=dict(zip(zd2["_id"].values.tolist(),zd2["hostname"].values.tolist()))
        relationship=[]
        for i in zd:
            x=zd1.get(i[0],None);y=zd2.get(i[1],None)
            if not (x and y):
                continue
            if x not in jh1 or y not in jh2:
                continue
            relationship.append((x,y))
        sql='''
        insert into topu.between_rack_and_server (rack_name,hostname) values (%s,%s);
        '''
        self.db_mysql_client.executemany(sql,relationship)
        self.db_mysql.client.commit()

    def create_storage_node(self):
        sql='''
        create table if not exists topu.storage (
            hostname VARCHAR(100) primary key,
            ip VARCHAR(25),
            controller_ip_1 VARCHAR(25),
            controller_ip_2 VARCHAR(25),
            controller_ip_3 VARCHAR(25),
            storage_group VARCHAR(25),
            storage_type VARCHAR(25),
            u_begin VARCHAR(25),
            u_height VARCHAR(25),
            u_front_back VARCHAR(25),
            u_size VARCHAR(25),
            dev_code VARCHAR(25),
            sn_code VARCHAR(25)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.storage;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        pipeline_copy=self.pipeline.copy()
        pipeline_copy.append(
            {
                '$project':{
                    "hostname":1,
                    "device_ip":1,
                    "controller_ip_1":1,
                    "controller_ip_2":1,
                    "controller_ip_3":1,
                    "device_storage_group":1,
                    "device_storage_type":1,
                    "u_begin":1,
                    "u_height":1,
                    "u_front_back":1,
                    "u_size":1,
                    "dev_code":1,
                    "sn_code":1,
                    "update_at":1
                }
            }
        )
        temp=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_storage.aggregate(pipeline_copy))).astype(str)[["hostname","device_ip","controller_ip_1","controller_ip_2","controller_ip_3","device_storage_group","device_storage_type","u_begin","u_height","u_front_back","u_size","dev_code","sn_code","update_at"]].values.tolist()
        zd=self.db_mongo.get_collection("cds_dict_detail",{"status":1},{"_id":1,"field_name":1})
        zd=dict(zip(zd["_id"].values.tolist(),zd["field_name"].values.tolist()))
        storage={}
        for i in range(len(temp)):
            hostname=temp[i][0]
            hostname="-".join([i.strip() for i in hostname.split("-")])
            temp[i][0]=hostname
            for j in range(1,5):
                if "." not in temp[i][j]:
                    temp[i][j]=""
                else:
                    temp[i][j]=".".join(i.strip() for i in temp[i][j].split("."))
            for j in [5,6,9,10]:
                try:
                    x=temp[i][j]
                    x=x[x.index("'")+1:]
                    x=x[:x.index("'")]
                    x=zd.get(x,"")
                except:
                    x=""
                temp[i][j]=x
            for j in [7,8]:
                try:
                    temp[i][j]=str(int(temp[i][j]))
                except:
                    temp[i][j]=""
            if temp[i][0] not in storage:
                storage[temp[i][0]]=temp[i]
            elif storage[temp[i][0]][-1]<=temp[i][-1]:
                storage[temp[i][0]]=temp[i]
        storage=[tuple(i[:-1]) for i in storage.values()]
        sql='''
        insert into topu.storage (hostname,ip,controller_ip_1,controller_ip_2,controller_ip_3,storage_group,storage_type,u_begin,u_height,u_front_back,u_size,dev_code,sn_code) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        self.db_mysql_client.executemany(sql,storage)
        self.db_mysql.client.commit()

    def create_edge_between_rack_and_storage(self):
        sql='''
        create table if not exists topu.between_rack_and_storage (
            rack_name VARCHAR(100),
            hostname VARCHAR(100),
            primary key (rack_name,hostname)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.between_rack_and_storage;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        zd=self.db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"storage"},{"rack_id":1,"storage_id":1})[["rack_id","storage_id"]].values.tolist()
        jh1=set(self.db_mysql.get_table_data("","select rack_name from topu.rack")["rack_name"].values.tolist())
        jh2=set(self.db_mysql.get_table_data("","select hostname from topu.storage")["hostname"].values.tolist())
        zd1=self.db_mongo.get_collection("cds_ci_att_value_rack",{"status":1},{"_id":1,"data_center_name":1,"room_name":1,"rack_name":1})
        pipeline_copy=self.pipeline.copy()
        pipeline_copy.append(
            {
                '$project':{
                    "_id":1,
                    "hostname":1
                }
            }
        )
        zd2=pd.DataFrame(list(self.db_mongo.db.cds_ci_att_value_storage.aggregate(pipeline_copy))).astype(str)
        zd1=dict(zip(zd1["_id"].values.tolist(),[i[0]+"|"+i[1]+"|"+i[2]+"|rack" for i in zd1[["data_center_name","room_name","rack_name"]].values.tolist()]))
        zd2=dict(zip(zd2["_id"].values.tolist(),zd2["hostname"].values.tolist()))
        relationship=[]
        for i in zd:
            x=zd1.get(i[0],None);y=zd2.get(i[1],None)
            if not (x and y):
                continue
            if x not in jh1 or y not in jh2:
                continue
            relationship.append((x,y))
        sql='''
        insert into topu.between_rack_and_storage (rack_name,hostname) values (%s,%s);
        '''
        self.db_mysql_client.executemany(sql,relationship)
        self.db_mysql.client.commit()

    def create_interface_node(self):
        sql='''
        create table if not exists topu.interface (
            hostname VARCHAR(100),
            name VARCHAR(100),
            primary key (hostname,name)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.interface;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        network=set(self.db_mysql.get_table_data("","select hostname from topu.network")["hostname"].values.tolist())
        temp=self.db_mongo.get_collection("cds_ci_att_value_interface",{"status":1},{"hostname":1,"name":1,"update_at":1})[["hostname","name","update_at"]].values.tolist()
        interface={}
        for i in temp:
            if i[0] not in network:
                continue
            if (i[0],i[1]) not in interface:
                interface[(i[0],i[1])]=i[-1]
            elif interface[(i[0],i[1])]<=i[-1]:
                interface[(i[0],i[1])]=i[-1]
        sql='''
        insert into topu.interface (hostname,name) values (%s,%s);
        '''
        self.db_mysql_client.executemany(sql,interface)
        self.db_mysql.client.commit()

    def create_edge_between_network_and_interface(self):
        sql='''
        create table if not exists topu.between_network_and_interface (
            hostname VARCHAR(100),
            name VARCHAR(100),
            primary key (hostname,name)
        );
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        sql='''
        truncate table topu.between_network_and_interface;
        '''
        self.db_mysql_client.execute(sql)
        self.db_mysql.client.commit()
        network=set(self.db_mysql.get_table_data("","select hostname from topu.network")["hostname"].values.tolist())
        temp=self.db_mongo.get_collection("cds_ci_att_value_interface",{"status":1},{"hostname":1,"name":1,"update_at":1})[["hostname","name","update_at"]].values.tolist()
        interface={}
        for i in temp:
            if i[0] not in network:
                continue
            if (i[0],i[1]) not in interface:
                interface[(i[0],i[1])]=i[-1]
            elif interface[(i[0],i[1])]<=i[-1]:
                interface[(i[0],i[1])]=i[-1]
        sql='''
        insert into topu.between_network_and_interface (hostname,name) values (%s,%s);
        '''
        self.db_mysql_client.executemany(sql,interface)
        self.db_mysql.client.commit()

    def run(self):
        self.create_area_node()
        self.create_country_node()
        self.create_edge_between_area_and_country()
        self.create_city_node()
        self.create_edge_between_country_and_city()
        self.create_data_center_node()
        self.create_edge_between_city_and_data_center()
        self.create_room_node()
        self.create_edge_between_data_center_and_room()
        self.create_rack_node()
        self.create_edge_between_room_and_rack()
        self.create_network_node()
        self.create_edge_between_rack_and_network()
        self.create_server_node()
        self.create_edge_between_rack_and_server()
        self.create_storage_node()
        self.create_edge_between_rack_and_storage()
        self.create_interface_node()
        self.create_edge_between_network_and_interface()

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
        "mongodb":{
            "HOST":"10.216.141.46",
            "PORT":27017,
            "USERNAME":"manager",
            "PASSWORD":"cds-cloud@2017"
        }
    }
    m=Run(config1,config2)
    m.run()