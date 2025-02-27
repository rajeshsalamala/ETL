import os
import streamlit as st
from pyspark.sql.functions import col,upper
from func.objects import spark
from func.common_functions import get_properties,warning_image
from func.custom_transform import transform_data


global columns
columns = True


def choose_columns(df):
    if df is not None:
        global selected_columns_count
        global columns
        try:
            all_columns = df.columns
            # st.title("Choose columns to Transformation")
            st.markdown("<h3 style='color:#144488;'>Select Columns for Transformation</h3>", unsafe_allow_html=True)
            selected_columns = st.multiselect('', all_columns)
            if selected_columns != []:
                selected_df = df.select(*[col(column_name)for column_name in selected_columns])
                st.dataframe(selected_df)
                columns = True
                return selected_columns
            else:
                return None
        except:
            columns = False
            return None
    else:
        print("Nooooooooooooooooooooooo")
    

# Function to add JavaScript to clear text box on click
def clear_text_on_click(text_input_id):
    return f"""
        <script>
            var textBox = document.getElementById("{text_input_id}");
            textBox.value = "";
            textBox.onclick = null;
        </script>
    """
    
def transform_form():
    di = {}

    with st.form("my_form"):
        spaces         = st.checkbox("Remove Extra Spaces")
        digits_splchar = st.checkbox("Replace Digits and Special Characters")
        duplicates     = st.checkbox("Remove Duplicate Rows(EntireData)")

        st.markdown("<h6 style='color:#144488;'>Repace With Text </h6>", unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        # Text input for the first column
        with col1:
            ip_text = st.text_input("Original Text:")
        # Text input for the second column
        with col2:
            op_text = st.text_input("Replace Text:")

        st.markdown("<h6 style='color:#144488;'>Repace With Regular Expression </h6>", unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        # Text input for the first column
        with col1:
            ip_regex = st.text_input("Input Regex:")
        # Text input for the second column
        with col2:
            op_regex = st.text_input("Replace Regex:")


        # Submit button
        submitted = st.form_submit_button("Submit")

    # Display the submitted information after the form is submitted
    if submitted:
        temp_di = {"text_repace":False,"ip_text": ip_text, "op_text": op_text,"regex_repace":False, "ip_regex": ip_regex,
                    "op_regex": op_regex,"spaces": spaces,"digits_splchar": digits_splchar,"duplicates": duplicates }
        temp_di['text_repace'] = True if len(ip_text)> 0 else False
        temp_di['regex_repace'] = True if len(ip_regex)> 0 else False

        di.update(temp_di)
        return di


def main():
    current_dir = os.getcwd()
    temp_dir = current_dir+'/temp/'
    # read Data with input properties
    ip_props = get_properties()['input_file']               
    df = spark.read.csv(temp_dir+'temp.csv',header=True)
    # Use the select() method to apply the upper() function to all columns
    df = df.select(*[col(column).alias(column.upper()) for column in df.columns])
    for col_name in df.columns:
        df = df.withColumn(col_name, upper(col(col_name)))


    sel_cols = choose_columns(df)
    if not columns :
        im = warning_image
        st.image(im, caption='Warning: Data Not Selected', use_column_width=False)
    if sel_cols is not None and len(sel_cols) !=[]:
        methods = transform_form()
        if methods:
            df_transformed = transform_data(df,methods,sel_cols)
            pandas_df = df_transformed.toPandas()
            temp_dir = os.path.join(os.getcwd(), "temp")
            pandas_df.to_csv(temp_dir+'/temp.csv',index=None,header=True) 
            # Display the transformed DataFrame
            st.header("Transformed DataFrame")
            st.write(pandas_df)





if __name__ == "__main__":
    main()
