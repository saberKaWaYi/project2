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

handler=get_rotating_handler("sql_demo.log")
logging_sql_demo=logging.getLogger("sql_demo")
logging_sql_demo.setLevel(logging.INFO)
logging_sql_demo.addHandler(handler)

import time

class Create:
    
    def __init__(self):
        self.max_times=3
        self.sleep_time=1
        self.x="\'"
        self.y="\""

    def drop_space(self,nebula,name):
        for _ in range(self.max_times):
            drop_space_statement=f"DROP SPACE IF EXISTS {name};"
            res=nebula.execute(drop_space_statement)
            if res.is_succeeded():
                logging_sql_demo.info("删除图数据库成功。")
                return
            time.sleep(self.sleep_time)
        logging_sql_demo.error(f"删除图数据库失败。{drop_space_statement}。{res.error_msg()}。")
        raise Exception(f"删除图数据库失败。{drop_space_statement}。{res.error_msg()}。")

    def create_space(self,nebula,space_name,partition_num=100,replica_factor=1,vid_type="FIXED_STRING(128)"):
        for _ in range(self.max_times):
            create_space_statement=f"CREATE SPACE IF NOT EXISTS {space_name} (partition_num={partition_num}, replica_factor={replica_factor}, vid_type={vid_type});"
            res=nebula.execute(create_space_statement)
            if res.is_succeeded():
                logging_sql_demo.info("创建图数据库成功。")
                return
            time.sleep(self.sleep_time)
        logging_sql_demo.error(f"创建图数据库失败。{create_space_statement}。{res.error_msg()}。")
        raise Exception(f"创建图数据库失败。{create_space_statement}。{res.error_msg()}。")

    def create_tag(self,nebula,tag_name,tag_properties):
        for _ in range(self.max_times):
            property_str=", ".join([f"{key} {value}" for key,value in tag_properties.items()])
            create_space_statement=f"CREATE TAG IF NOT EXISTS {tag_name}({property_str});"
            res=nebula.execute(create_space_statement)
            if res.is_succeeded():
                logging_sql_demo.info(f"创建tag:{tag_name}成功。")
                return
            time.sleep(self.sleep_time)
        logging_sql_demo.error(f"创建tag:{tag_name}失败。{create_space_statement}。{res.error_msg()}。")
        raise Exception(f"创建tag:{tag_name}失败。{create_space_statement}。{res.error_msg()}。")
    
    def create_nodes(self,nebula,tag_name,vid_names,df_all,chunk_size=1000):
        for begin_index in range(0,df_all.shape[0],chunk_size):
            end_index=min(begin_index+chunk_size,df_all.shape[0])
            df=df_all.iloc[begin_index:end_index]
            for _ in range(self.max_times):
                create_nodes_statement=f"INSERT VERTEX IF NOT EXISTS {tag_name} "
                temp1=[i for i in df.columns if i not in vid_names]
                create_nodes_statement+=f"({', '.join(temp1)}) VALUES "
                for i in range(df.shape[0]):
                    vid="|".join([df[j].values[i] for j in vid_names])
                    create_nodes_statement+="\""+vid+"\""+":("
                    temp2=["\""+df[j].values[i]+"\"" for j in temp1]
                    create_nodes_statement+=", ".join(temp2)
                    create_nodes_statement+="), "
                create_nodes_statement=create_nodes_statement[:-2]
                create_nodes_statement+=";"
                res=nebula.execute(create_nodes_statement)
                if res.is_succeeded():
                    logging_sql_demo.info(f"批量创建{tag_name}|{begin_index//chunk_size:<6}成功了。")
                    break
                time.sleep(self.sleep_time)
            else:
                logging_sql_demo.error(f"批量创建{tag_name}失败了。{create_nodes_statement}。{res.error_msg()}。")
                raise Exception(f"批量创建{tag_name}失败了。{create_nodes_statement}。{res.error_msg()}。")

    def create_edge_type(self,nebula,edge_type1,edge_type2):
        for _ in range(self.max_times):
            create_edge_type_statement=f"CREATE EDGE IF NOT EXISTS {edge_type1} (name string, type {edge_type2});"
            res=nebula.execute(create_edge_type_statement)
            if res.is_succeeded():
                logging_sql_demo.info(f"创建{edge_type1}成功。")
                return
            time.sleep(self.sleep_time)
        logging_sql_demo.error(f"创建{edge_type1}失败。{create_edge_type_statement}。{res.error_msg()}。")
        raise Exception(f"创建{edge_type1}失败。{create_edge_type_statement}。{res.error_msg()}。")
    
    def create_edges(self,nebula,edge_type,data_all,chunk_size):
        for begin_index in range(0,len(data_all),chunk_size):
            end_index=min(begin_index+chunk_size,len(data_all))
            data=data_all[begin_index:end_index]
            for _ in range(self.max_times):
                create_edges_statement=f"INSERT EDGE IF NOT EXISTS {edge_type} (name, info) values "
                for i in data:
                    create_edges_statement+=f"\"{i[0]}\"->\"{i[1]}\":(\"{str(i[0])}_to_{str(i[1])}\", \"{i[2]}\")"
                    create_edges_statement+=", "
                create_edges_statement=create_edges_statement[:-2]
                res=nebula.execute(create_edges_statement)
                if res.is_succeeded():
                    logging.info(f"批量创建{edge_type}|{begin_index//chunk_size:<6}成功了。")
                    break
                time.sleep(self.sleep_time)
            else:
                logging.error(f"批量创建{edge_type}失败了。{create_edges_statement}。{res.error_msg()}。")
                raise Exception(f"批量创建{edge_type}失败了。{create_edges_statement}。{res.error_msg()}。")