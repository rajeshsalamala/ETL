import os
import streamlit as st
from io import BytesIO
from zipfile import ZipFile
from pyspark.sql.functions import col,upper
from func.objects import spark
from func.io_functions import read_data
from func.profile import data_profiling,groupBy_conditions
from func.common_functions import save_properties,get_properties,warning_image,warning_message_with_icon,auto_id




try:
    # read Data with input properties
    ip_props = get_properties()['input_file']               
    df = read_data(spark,ip_props)
    df = auto_id(df,spark)
    df_columns = df.columns
    # Use the select() method to apply the upper() function to all columns
    df = df.select(*[col(column).alias(column.upper()) for column in df_columns])
    for col_name in df_columns:
        df = df.withColumn(col_name, upper(col(col_name)))
except:
    pass


global selected_columns_count
global columns
global temp_dir

current_dir = os.getcwd()
temp_dir = current_dir+'/temp/'
columns = True
selected_columns_count = 0

def choose_columns():
    global selected_columns_count
    global columns
    try:
        all_columns = df.columns
        # st.markdown("<h3 style='color:#144488;'>Select Columns for Profiling</h3>", unsafe_allow_html=True)
        # selected_columns = st.multiselect('', all_columns)
        st.markdown("<h3 style='color:#144488;'>Select Columns for Profiling</h3>", unsafe_allow_html=True)
        selected_columns = st.multiselect('', all_columns)

        if selected_columns != []:
            selected_df = df.select(*[col(column_name)for column_name in selected_columns])
            st.dataframe(selected_df)
            columns = True
            selected_columns_count = len(selected_columns)
            return selected_columns
        else:
            return None
    except:
        columns = False
        return None
        
def map_columns():
    all_columns = choose_columns()
    if not columns :
        im = warning_image
        st.image(im, caption='Warning: Data Not Selected', use_column_width=False)
    if selected_columns_count !=0:
        st.title('Columns Mapping')
        ori_columns = ['Name', 'DOB', 'Phone', 'Email',"PINCODE","ADDRESS"]     
        selected_keys = st.multiselect('Maping Columns', ori_columns)
        selected_values = st.multiselect('Column Names in file/db', all_columns)
        limit_options = ['Select Limit',50, 100, 500, 1000]
        limit = st.selectbox('Select Limit Options', limit_options)
        di = dict(zip(selected_keys,selected_values))

        # if di !={}:
        #     return di
            
        if limit != 'Select Limit':
            di['limit'] = limit
            # print(di)
            return di
        else:
            return None

# def select_file_choice():
#     mapping = map_columns()
#     if (mapping is not None) and (ip_props !={}):
#         mapping.update(ip_props)
#         return mapping
#     elif mapping is not None:
#         profile_details = profil_table()
#         mapping.update(profile_details)
#         return mapping
#     else:
#         return None


def profil_table():
    prof_op_props = {}
    st.title("Enter Profiling DataBase Detail to process")
    db_name  = st.text_input("Profiling Database Name") 
    username = st.text_input("User Name")
    password = st.text_input("Password",type="password")
    db_host = st.text_input("Host")
    db_port = st.text_input("Port",3306)
    prof_op_write_mode = st.text_input("Write Mode",'append')

    temp_di = {"op_type":"database","database":db_name,"username":username,"op_write_mode":prof_op_write_mode,
                "password":password,"host":db_host,"db_port":db_port}
    prof_op_props.update(temp_di)

    db_submitted = st.button("Updata Table")
    if db_submitted:
        if not prof_op_props['database']:
            st.error("Please enter db_name")
        if not prof_op_props['username']:
            st.error("Please enter username")
        if not prof_op_props['password']:
            st.error("Please enter password")
        if not prof_op_props['host']:
            st.error("Please enter Host")
        else:
            res = save_properties(prof_op_props,"profile_db")
            if res:
                st.write("Profiling Db details submitted!")
                return prof_op_props
            else:
                st.write("Failed Writing Profiling details in Property file")
                return False

import base64

def display_pdf(pdf_path):
    with open(pdf_path, "rb") as f:
        pdf_data = f.read()
    pdf_base64 = base64.b64encode(pdf_data).decode('utf-8')
    st.markdown(f'<iframe src="data:application/pdf;base64,{pdf_base64}" width="700" height="1000"></iframe>', unsafe_allow_html=True)


def main():
    map_data = map_columns()
    if map_data is not None:
        limit = map_data['limit']
        map_data.pop('limit')
        print(map_data)
        if map_data is not None:
            analyse_submitted = st.button("Analise")
            if analyse_submitted:
                # try:
                res = data_profiling(df,map_data)

                if res['Status']:

                    group_data = groupBy_conditions(res['data'],map_data,limit)

                    with open(temp_dir+'profiling_report.pdf', 'rb') as f:
                        pdf_bytes = f.read()

                    buf = BytesIO()
                    
                    with ZipFile(buf, "x") as zip:
                        # Iterate through the keys in the result data dictionary
                        zip.writestr("profiling_report.pdf", pdf_bytes)


                        for key in list(group_data.keys()):
                            # Convert each DataFrame to CSV string and add it to the zip file
                            csv_file = group_data[key].to_csv(index=False)
                            zip.writestr(f"{key}.csv", csv_file)

                        

                        # Provide the path to your PDF file
                        pdf_path = temp_dir+'profiling_report.pdf'
                        # Display the PDF
                        display_pdf(pdf_path)

                    st.download_button(
                        label="Download zip",
                        data=buf.getvalue(),
                        file_name="mydownload.zip",
                        mime="application/zip",)
                    # spark.stop()


                # except Exception as e:
                #     warning_message_with_icon("Pleasse Check for columns mapping")

if __name__ == "__main__":
    main()







