import os
import logging
from logging.handlers import RotatingFileHandler
from connect import Connect_Mysql

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

class Create_Table:

    def __init__(self,config):
        self.db_mysql=Connect_Mysql(config)
        self.cursor=self.db_mysql.client.cursor()

    def init(self):
        lt=["network","server","storage","interface","interface_to_interface","interface_between_nic","podx_clux_all_ipv4","area",\
            "country","city","area_between_country","country_between_city","data_center","city_between_data_center","room","rack",\
            "rack_between_network","rack_between_server","rack_between_storage","pool","cluster","volume","vm","pod"]
        for i in lt:
            self.cursor.execute(f"DROP TABLE IF EXISTS {i};")
            self.db_mysql.client.commit()

    def run(self):
        self.create_network()
        self.create_server()
        self.create_storage()
        self.create_interface()
        self.create_interface_to_interface()
        self.create_interface_between_nic()
        self.create_podx_clux_all_ipv4()
        ##################################################
        self.create_area()
        self.create_country()
        self.create_city()
        self.create_area_between_country()
        self.create_country_between_city()
        self.create_data_center()
        self.create_city_between_data_center()
        self.create_room()
        self.create_rack()
        self.create_rack_between_network()
        self.create_rack_between_server()
        self.create_rack_between_storage()
        ##################################################
        self.create_pool()
        self.create_cluster()
        self.create_volume()
        self.create_vm()
        ##################################################
        self.create_pod()

    def create_network(self):
        sql="""
        CREATE TABLE IF NOT EXISTS network (
            hostname VARCHAR(50) PRIMARY KEY,
            sn_code VARCHAR(100),
            patch_code VARCHAR(50),
            device_ip VARCHAR(50),
            device_ip_mask VARCHAR(25),
            snmp_ip VARCHAR(25),
            model VARCHAR(100),
            system_version VARCHAR(50),
            brand VARCHAR(10),
            update_at DATETIME(3)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_server(self):
        sql="""
        CREATE TABLE IF NOT EXISTS server (
            hostname VARCHAR(50) PRIMARY KEY,
            sn_code VARCHAR(50),
            device_ip VARCHAR(100),
            device_ip_mask VARCHAR(25),
            out_band_ip VARCHAR(100),
            out_band_ip_mask VARCHAR(25),
            cluster_ip_1 VARCHAR(25),
            cluster_ip_2 VARCHAR(25),
            cluster_ip_2_mask VARCHAR(5),
            storage_ip VARCHAR(25),
            storage_ip_mask VARCHAR(25),
            vmotion_ip_1 VARCHAR(25),
            vmotion_ip_1_mask VARCHAR(25),
            vmotion_ip_2 VARCHAR(25),
            vmotion_ip_2_mask VARCHAR(25),
            login_username VARCHAR(5),
            login_password VARCHAR(50),
            model VARCHAR(25),
            system_version VARCHAR(25),
            memorysummary VARCHAR(25),
            processorsummary VARCHAR(50),
            vc_host_memory VARCHAR(25),
            vc_host_model VARCHAR(50),
            vc_host_vendor VARCHAR(25),
            brand VARCHAR(25),
            update_at DATETIME(3)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_storage(self):
        sql="""
        CREATE TABLE IF NOT EXISTS storage (
            hostname VARCHAR(50) PRIMARY KEY,
            sn_code VARCHAR(25),
            dev_code VARCHAR(25),
            device_ip VARCHAR(25),
            device_ip_mask VARCHAR(25),
            out_band_ip VARCHAR(25),
            out_band_ip_mask VARCHAR(25),
            controller_ip_1 VARCHAR(25),
            controller_ip_1_mask VARCHAR(25),
            controller_ip_2 VARCHAR(25),
            controller_ip_2_mask VARCHAR(25),
            controller_ip_3 VARCHAR(25),
            controller_ip_3_mask VARCHAR(25),
            system_version VARCHAR(25),
            update_at DATETIME(3)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_interface(self):
        sql="""
        CREATE TABLE IF NOT EXISTS interface (
            hostname VARCHAR(50),
            name VARCHAR(100),
            device_id VARCHAR(10),
            high_speed VARCHAR(10),
            hit_level_alert VARCHAR(5),
            hit_level_critical VARCHAR(5),
            update_at DATETIME(3),
            PRIMARY KEY (hostname,name)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_interface_to_interface(self):
        sql="""
        CREATE TABLE IF NOT EXISTS interface_to_interface (
            hostname VARCHAR(50),
            device_ip VARCHAR(50),
            brand VARCHAR(10),
            lldpLocChassisId VARCHAR(25),
            lldpLocSysDesc TEXT,
            lldpLocPortId VARCHAR(50),
            lldpLocPortDesc TEXT,
            lldpRemChassisId TEXT,
            lldpRemPortId VARCHAR(150),
            lldpRemPortDesc TEXT,
            lldpRemSysName VARCHAR(100),
            lldpRemSysDesc TEXT,
            PRIMARY KEY (hostname,lldpLocPortId)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_interface_between_nic(self):
        sql="""
        CREATE TABLE IF NOT EXISTS interface_between_nic (
            hostname VARCHAR(50),
            device_ip VARCHAR(25),
            brand VARCHAR(25),
            nic VARCHAR(25),
            function VARCHAR(25),
            sysname VARCHAR(50),
            port VARCHAR(50),
            PRIMARY KEY (hostname,nic)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_podx_clux_all_ipv4(self):
        sql="""
        CREATE TABLE IF NOT EXISTS podx_clux_all_ipv4 (
            podx_clux VARCHAR(50) PRIMARY KEY,
            all_ipv4 MEDIUMTEXT
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_area(self):
        sql="""
        CREATE TABLE IF NOT EXISTS area (
            area_name VARCHAR(50) PRIMARY KEY
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_country(self):
        sql="""
        CREATE TABLE IF NOT EXISTS country (
            country_name VARCHAR(50) PRIMARY KEY
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_city(self):
        sql="""
        CREATE TABLE IF NOT EXISTS city (
            city_name VARCHAR(50) PRIMARY KEY
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_area_between_country(self):
        sql="""
        CREATE TABLE IF NOT EXISTS area_between_country (
            area_name VARCHAR(50),
            country_name VARCHAR(50),
            PRIMARY KEY (area_name,country_name)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_country_between_city(self):
        sql="""
        CREATE TABLE IF NOT EXISTS country_between_city (
            country_name VARCHAR(50),
            city_name VARCHAR(50),
            PRIMARY KEY (country_name,city_name)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_data_center(self):
        sql="""
        CREATE TABLE IF NOT EXISTS data_center (
            data_center_name VARCHAR(50) PRIMARY KEY,
            address TEXT,
            latitude VARCHAR(50),
            longitude VARCHAR(50),
            nick_name VARCHAR(50),
            type_name VARCHAR(25)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_city_between_data_center(self):
        sql="""
        CREATE TABLE IF NOT EXISTS city_between_data_center (
            city_name VARCHAR(50),
            data_center_name VARCHAR(50),
            PRIMARY KEY (city_name,data_center_name)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_room(self):
        sql="""
        CREATE TABLE IF NOT EXISTS room (
            room_name VARCHAR(100) PRIMARY KEY,
            length VARCHAR(25),
            width VARCHAR(25)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_rack(self):
        sql="""
        CREATE TABLE IF NOT EXISTS rack (
            rack_name VARCHAR(125) PRIMARY KEY,
            rack_status VARCHAR(5),
            rack_all_height VARCHAR(5),
            rack_xy VARCHAR(5),
            rack_up_or_down VARCHAR(5),
            power_unit VARCHAR(5),
            std_quantity VARCHAR(5),
            add_power_quantity VARCHAR(10)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_rack_between_network(self):
        sql="""
        CREATE TABLE IF NOT EXISTS rack_between_network (
            rack_name VARCHAR(125),
            hostname VARCHAR(50),
            PRIMARY KEY (rack_name,hostname)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_rack_between_server(self):
        sql="""
        CREATE TABLE IF NOT EXISTS rack_between_server (
            rack_name VARCHAR(125),
            hostname VARCHAR(50),
            PRIMARY KEY (rack_name,hostname)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_rack_between_storage(self):
        sql="""
        CREATE TABLE IF NOT EXISTS rack_between_storage (
            rack_name VARCHAR(125),
            hostname VARCHAR(50),
            PRIMARY KEY (rack_name,hostname)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_pool(self):
        sql="""
        CREATE TABLE IF NOT EXISTS pool (
            pool_name VARCHAR(50) PRIMARY KEY
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_cluster(self):
        sql="""
        CREATE TABLE IF NOT EXISTS cluster (
            cluster_name VARCHAR(100) PRIMARY KEY
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_volume(self):
        sql="""
        CREATE TABLE IF NOT EXISTS volume (
            volume_name VARCHAR(100) PRIMARY KEY
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()
    
    def create_vm(self):
        sql="""
        CREATE TABLE IF NOT EXISTS vm (
            vm_name VARCHAR(200) PRIMARY KEY
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

    def create_pod(self):
        sql="""
        CREATE TABLE IF NOT EXISTS pod (
            pod_name VARCHAR(25) PRIMARY KEY,
            data_center_name VARCHAR(50)
        );
        """
        self.cursor.execute(sql)
        self.db_mysql.client.commit()

if __name__=="__main__":
    config={
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
    m=Create_Table(config)
    m.init()
    m.run()