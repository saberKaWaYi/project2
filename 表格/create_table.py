from connect import Connect_Mysql

def create_table(config):
    db_mysql=Connect_Mysql(config)
    cursor=db_mysql.client.cursor()
    sql="""
    CREATE TABLE IF NOT EXISTS cds_report.report_network_lldp (
        hostname VARCHAR(100),
        ip VARCHAR(25),
        brand VARCHAR(25),
        switch TINYINT(1),
        dt DATE,
        info TEXT,
        region VARCHAR(25)
    );
    """
    cursor.execute(sql)
    db_mysql.client.commit()

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
    create_table(config)