import streamlit as st
import os
from func.io_functions import read_data
from func.objects import styles,spark
from func.common_functions import config,save_properties,remove_files_from_folder,clear_property_file
# from ydata_profiling import ProfileReport
import streamlit.components.v1 as components


def main():
    print('*'*50)

    file_path = os.getcwd()+'/temp'
    file_location = ''
    # Clear Temp folder every time we click Data Ingestion Page
    remove_files_from_folder(file_path)
    # Example usage to clear the property file
    property_file_path = os.getcwd()+"/configration.properties"
    clear_property_file(property_file_path)


    global ip_props

    ip_props = {}
    file_submit = False

    with st.form("my_form"):
        # st.title("Data Ingestion Details")
        st.markdown("<h1 style='color:#144488;'>Data Ingestion</h1>", unsafe_allow_html=True)

        # st.write("Enter Input Information:")


        options = ['Select Input Type','File', 'Database']
        ip_selected_option = st.selectbox('Select Input Type', options)

        submitted = st.form_submit_button("Proceed")

        if ip_selected_option == 'File':
            options = ['Select File Format Type','csv', 'parquet','txt','json','avro','orc']
            property1_option = st.selectbox('Select Input File Format', options)

            ftype_submitted = st.form_submit_button("Proceed.") 
            

            if property1_option == 'csv' or property1_option == 'txt':

                uploaded_file = st.file_uploader("Upload a file")
                
                if uploaded_file is not None:
                    # Create a temporary directory
                    temp_dir = os.path.join(os.getcwd(), "temp")
                    os.makedirs(temp_dir, exist_ok=True)
                    # Save the uploaded file to the temporary directory
                    file_location = os.path.join(temp_dir, uploaded_file.name)
                    with open(file_location, "wb") as file:
                        file.write(uploaded_file.getvalue())
                    # Display a success message
                    st.success("File saved to temporary directory.")
                else:
                    print('File upload Failed')

                options = ['True', 'False']
                header_option = st.selectbox('Select header option', options)

                options = [',', '|','space','.','tab','Other']
                delimiter_option = st.selectbox('Select delimiter', options)
                if delimiter_option == 'Other':
                    custom_delimiter = st.text_input('Enter Custom Delimiter')
                    temp_di = {"ip_type":"file", "file_type":property1_option,"ip_file_path":file_location,
                            "header":header_option,"delimiter":custom_delimiter}
                    ip_props.update(temp_di)      
                else:
                    temp_di = {"ip_type":"file", "file_type":property1_option,"ip_file_path":file_location,
                            "header":header_option,"delimiter":delimiter_option}
                    ip_props.update(temp_di)    


                file_submitted = st.form_submit_button("Proceed...")
                if file_submitted:
                    if ip_selected_option == 'File':
                        if not ip_props['ip_file_path']:
                            st.error("Please upload file to process...!!!")
                        if ip_props['delimiter']=='':
                            st.error("Please Enter Custom Delimiter...!!!")
                        else:
                            file_submit = True
                            res = save_properties(ip_props,'input_file')
                            if res:
                                st.write("Form submitted!")
                            else:
                                st.write("Failed Writing Property file")

            if property1_option == 'parquet' or property1_option == 'json' or property1_option == 'avro' or property1_option == 'orc':
                uploaded_file = st.file_uploader("Upload a file")
                
                if uploaded_file is not None:
                    # Create a temporary directory
                    temp_dir = os.path.join(os.getcwd(), "temp")
                    os.makedirs(temp_dir, exist_ok=True)
                    # Save the uploaded file to the temporary directory
                    file_location = os.path.join(temp_dir, uploaded_file.name)
                    with open(file_location, "wb") as file:
                        file.write(uploaded_file.getvalue())
                    # Display a success message
                    st.success("File saved to temporary directory.")
                else:
                    print('File upload Failed')
                temp_di = {"ip_type":"file", "file_type":property1_option,"ip_file_path":file_location,
                    "header":'',"delimiter":''}
                ip_props.update(temp_di)

                file_submitted = st.form_submit_button("Proceed...")
                if file_submitted:
                    if ip_selected_option == 'File':
                        if not ip_props['ip_file_path']:
                            st.error("Please upload file to process...!!!")
                        else:
                            file_submit = True
                            res = save_properties(ip_props,'input_file')
                            if res:
                                st.write("Form submitted!")
                            else:
                                st.write("Failed Writing Property file")

                else:
                    print('File upload Failed')


        elif ip_selected_option == 'Database':
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
                    file_submit = True
                    res = save_properties(ip_props,'input_file')
                    if res:
                        st.write("Form submitted!")
                    else:
                        st.write("Failed Writing Property file")


    if file_submit:
        # st.write(ip_props)
        # try:
        df = read_data(spark,ip_props)
        # st.dataframe(pandas_df)
        # table
        if df is not None:
            temp_dir = os.path.join(os.getcwd(), "temp")
            pandas_df = df.toPandas()
            pandas_df.to_csv(temp_dir+'/temp.csv',index=None,header=True) 
        
            sample_df = df.toPandas()
            df2=sample_df.style.set_properties(**{'text-align': 'left'}).set_table_styles(styles)
            # st.write("Data Ingestion Completed, Go to Data Profiling for Further...")
            st.write("Sample Data...")
            print(sample_df)
            st.dataframe(data=sample_df, width=None, height=None,hide_index=None, column_order=None, column_config=None)
            # temp_path = os.getcwd()+'/temp/'
            # html_file = temp_path+'temp_report.html'
            # # st.table(df2[:10])
            # profile = ProfileReport(sample_df, explorative=True)
            # profile.to_file(html_file)
            # # # Display the HTML report using an iframe
            # st.header('Profiling Report')
            # with open(html_file, 'r', encoding='utf-8') as f:
            #     report_content = f.read()
            # components.html(report_content, width=800, height=600, scrolling=True)
            return df
            # except:
            #     st.write({"Failed":df})
        else:
            st.warning({"result":"File-Type Not Matched"})


if __name__ == '__main__':
    main()





