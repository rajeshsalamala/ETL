import os


def get_file_type(file_path):
    file_extension = os.path.splitext(file_path)[1]
    if file_extension:
        return file_extension[1:].lower()
    else:
        return None
    
##################
# Data Reading  
##################

def read_data(spark,details_di):

    if details_di['ip_type'] == 'file':
        if os.path.exists(details_di["ip_file_path"]):
            file_path  = details_di["ip_file_path"]
            file_type  = details_di["file_type"]
            header     = details_di["header"]
            delimiter  = details_di["delimiter"]
            delimiter  = " " if delimiter=='space' else delimiter
            delimiter  = "\t" if delimiter=='tab' else delimiter

            loc_file_type = get_file_type(file_path)
            
            if file_type.lower() == loc_file_type.lower():
                file_type = 'csv' if file_type =='txt' else file_type
                file_type = 'com.databricks.spark.avro' if file_type =='avro' else file_type
                df = spark.read.option("header", f"{header}").option("delimiter", f"{delimiter}").format(file_type).load(file_path)
                return df
            else:
                return None
        else:
            return {"result":"File not found...!!!"}
    elif details_di['ip_type'] == 'database':
        if details_di["rdmbs_option"] == 'MySql':
            db_url          = f"jdbc:mysql://{details_di['host']}/{details_di['database']}"
            db_usr_name     = details_di['username']
            db_pwd          = details_di['password']
            db_driver       = 'com.mysql.cj.jdbc.Driver'
            read_mode       = "jdbc"
            ip_table_name   = details_di['ip_table_name']

            try:
                df = spark.read.format(read_mode).options(url=db_url,driver=db_driver,
                                                    dbtable=ip_table_name, user=db_usr_name,password=db_pwd)\
                                                    .load()
                return df
            except:
                return {"Failed":"Reading From Data base is Failed"}
            
         
        elif details_di["rdmbs_option"] == 'MSSql':
            db_url = f"jdbc:sqlserver://{details_di['host']}:{details_di['db_port']};databaseName={details_di['database']}"
            db_usr_name = details_di['username']
            db_pwd = details_di['password']
            db_driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
            read_mode = "jdbc"
            ip_table_name = details_di['ip_table_name']

            # try:
            df = spark.read.format(read_mode) \
                .options(url=db_url, driver=db_driver, dbtable=ip_table_name, user=db_usr_name, password=db_pwd) \
                .load()
            
            return df
            # except Exception as e:
            #     return {"Failed": f"Reading from database failed: {str(e)}"}

            
        elif details_di["rdmbs_option"] == 'Oracle':
            db_url          = f"jdbc:oracle:thin:@//{details_di['host']}:{details_di['db_port']}/{details_di['database']}"
            db_usr_name     = details_di['username']
            db_pwd          = details_di['password']
            db_driver       = 'oracle.jdbc.driver.OracleDriver'
            read_mode       = "jdbc"
            ip_table_name   = details_di['ip_table_name']

            try:
                df = spark.read.format(read_mode).options(url=db_url,driver=db_driver,
                                                    dbtable=ip_table_name, user=db_usr_name,password=db_pwd)\
                                                    .load()
                return df
            except:
                return {"Failed":"Reading From Data base is Failed"}
            
        elif details_di["rdmbs_option"] == 'PostgreSQL':
            db_url          = f"jdbc:postgresql://{details_di['host']}:{details_di['db_port']}/{details_di['database']}"
            db_usr_name     = details_di['username']
            db_pwd          = details_di['password']
            db_driver       = "org.postgresql.Driver"
            read_mode       = "jdbc"
            ip_table_name   = details_di['ip_table_name']

            try:
                df = spark.read.format(read_mode).options(url=db_url,driver=db_driver,
                                                    dbtable=ip_table_name, user=db_usr_name,password=db_pwd)\
                                                    .load()
                return df
            except:
                return {"Failed":"Reading From Data base is Failed"}

    else:
        return "FAILED FETCHING DATA"





##################
# Data Loading  
##################

from pyspark.sql import SparkSession

def write_to_database(spark_session,details_di, df):

    if details_di['ip_type'] == 'database':
        if details_di["rdmbs_option"] == 'MySql':
            db_url = f"jdbc:mysql://{details_di['host']}/{details_di['database']}"
            db_usr_name = details_di['username']
            db_pwd = details_di['password']
            db_driver = 'com.mysql.cj.jdbc.Driver'
            write_mode = "overwrite"  # You can change this based on your requirements
            ip_table_name = details_di['ip_table_name']

            try:
                df.write.mode(write_mode).format("jdbc").options(
                    url=db_url, driver=db_driver, dbtable=ip_table_name,
                    user=db_usr_name, password=db_pwd
                ).save()
                return {"Success": "Data written to the MySQL database successfully."}
            except Exception as e:
                return {"Failed": f"Writing to MySQL database failed. Error: {str(e)}"}

        elif details_di["rdmbs_option"] == 'MSSql':
            db_url = f"jdbc:sqlserver://{details_di['host']}:{details_di['db_port']};databaseName={details_di['database']}"
            db_usr_name = details_di['username']
            db_pwd = details_di['password']
            db_driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
            write_mode = "overwrite"  # You can change this based on your requirements
            ip_table_name = details_di['ip_table_name']

            try:
                df.write.mode(write_mode).format("jdbc").options(
                    url=db_url, driver=db_driver, dbtable=ip_table_name,
                    user=db_usr_name, password=db_pwd
                ).save()
                return {"Success": "Data written to the MySQL database successfully."}
            except Exception as e:
                return {"Failed": f"Writing to MySQL database failed. Error: {str(e)}"}
            
        elif details_di["rdmbs_option"] == 'Oracle':
            db_url = f"jdbc:oracle:thin:@//{details_di['host']}:{details_di['db_port']}/{details_di['database']}"
            db_usr_name = details_di['username']
            db_pwd = details_di['password']
            db_driver = 'oracle.jdbc.driver.OracleDriver'
            write_mode = "overwrite"  # You can change this based on your requirements
            ip_table_name = details_di['ip_table_name']

            try:
                df.write.mode(write_mode).format("jdbc").options(
                    url=db_url, driver=db_driver, dbtable=ip_table_name,
                    user=db_usr_name, password=db_pwd
                ).save()
                return {"Success": "Data written to the Oracle database successfully."}
            except Exception as e:
                return {"Failed": f"Writing to Oracle database failed. Error: {str(e)}"}

        elif details_di["rdmbs_option"] == 'PostgreSQL':
            db_url = f"jdbc:postgresql://{details_di['host']}:{details_di['db_port']}/{details_di['database']}"
            db_usr_name = details_di['username']
            db_pwd = details_di['password']
            db_driver = "org.postgresql.Driver"
            write_mode = "overwrite"  # You can change this based on your requirements
            ip_table_name = details_di['ip_table_name']

            try:
                df.write.mode(write_mode).format("jdbc").options(
                    url=db_url, driver=db_driver, dbtable=ip_table_name,
                    user=db_usr_name, password=db_pwd
                ).save()
                return {"Success": "Data written to the PostgreSQL database successfully."}
            except Exception as e:
                return {"Failed": f"Writing to PostgreSQL database failed. Error: {str(e)}"}



