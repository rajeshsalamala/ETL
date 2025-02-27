import streamlit as st
import os
from func.io_functions import write_to_database
from func.objects import spark
from func.common_functions import save_properties
import streamlit as st
import pandas as pd
import base64

# def download_link(df, filename='elt_data', format='csv', text='Download'):
#     if format == 'csv':
#         # CSV
#         csv = df.to_csv(index=False)
#         b64 = base64.b64encode(csv.encode()).decode()
#         href = f'data:file/csv;base64,{b64}'
#     elif format == 'json':
#         # JSON
#         json_data = df.to_json(orient='records')
#         b64 = base64.b64encode(json_data.encode()).decode()
#         href = f'data:file/json;base64,{b64}'
#     else:
#         raise ValueError("Unsupported format. Use 'csv' or 'json'")
    
#     # Trigger automatic download using JavaScript
#     st.markdown(f'<a href="{href}" download="{filename}.{format}" id="download_link"></a>', unsafe_allow_html=True)
#     st.markdown('<script>document.getElementById("download_link").click();</script>', unsafe_allow_html=True)
    
#     return href

def download_link(df, filename='elt_data', format='csv', text='Download'):
    if format == 'csv':
        # CSV
        csv = df.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64}" download="{filename}.csv">{text} (CSV)</a>'
    elif format == 'json':
        # JSON
        json_data = df.to_json(orient='records')
        b64 = base64.b64encode(json_data.encode()).decode()
        href = f'<a href="data:file/json;base64,{b64}" download="{filename}.json">{text} (JSON)</a>'
    else:
        raise ValueError("Unsupported format. Use 'csv' or 'json'")
    
    
    return href


def main():

    temp_dir = os.path.join(os.getcwd(), "temp")
    global ip_props
    ip_props={}

    with st.form("my_form"):
        # st.title("Data Ingestion Details")
        st.markdown("<h1 style='color:#144488;'>Data Loading</h1>", unsafe_allow_html=True)

        # st.write("Enter Input Information:")


        options = ['Select Load Type','File', 'Database']
        ip_selected_option = st.selectbox('Select Load Type', options)

        submitted = st.form_submit_button("Proceed")

        if ip_selected_option == 'File':
            df = pd.read_csv(temp_dir+"/temp.csv")
            options = ['Select File Format Type','csv', 'json']
            property1_option = st.selectbox('Select Input File Format', options)
            ftype_submitted = st.form_submit_button("Proceed.")

            if ftype_submitted:
                print(property1_option) 
                # Create a download link
                download_href = download_link(df, format=property1_option)
                st.markdown(download_href, unsafe_allow_html=True)


                        


        elif ip_selected_option == 'Database':
            df = spark.read.csv(temp_dir+'/temp.csv',header=True)

            # options = ['Select RDMBMS','MySql', 'DB2','Oracle']
            # rdmbs_option = st.selectbox('Select RDMBMS', options)

            options = ['Select RDBMS', 'MSSql','MySql', 'Oracle', 'PostgreSQL']
            rdmbs_option = st.selectbox('Select RDBMS', options)
            rdmbs_option = "" if rdmbs_option=="Select RDBMS" else rdmbs_option
            db_name = st.text_input("Input Database Name")
            username = st.text_input("User Name")
            password = st.text_input("Password",type="password")
            db_host = st.text_input("Host")
            db_port = st.text_input("Port",3306)
            ip_table_name = st.text_input("Table Name")

            temp_di = {"ip_type":"database","rdmbs_option":rdmbs_option,"database":db_name,"username":username,"password":password,"host":db_host,
                    "db_port":db_port,"ip_table_name":ip_table_name}
            ip_props.update(temp_di)

            db_submitted = st.form_submit_button("Proceed...")
            if db_submitted:
                if not ip_props['database']:
                    st.error("Please enter db_name")
                if not ip_props['username']:
                    st.error("Please enter username")
                if not ip_props['password']:
                    st.error("Please enter password")
                if not ip_props['host']:
                    st.error("Please enter Host")
                if not ip_props['ip_table_name']:
                    st.error("Please enter Table Name")
                if  not ip_props['rdmbs_option']:
                    st.error("Please select an RDBMS from the dropdown.")

                else:
                    res = save_properties(ip_props,'input_file')
                    if res:
                        st.write("Form submitted!",ip_props)
                        write_res = write_to_database(spark,ip_props,df)
                        st.write(write_res)
                    else:
                        st.write("Failed Writing Property file")





if __name__ == '__main__':
    main()



