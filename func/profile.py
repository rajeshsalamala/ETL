from func.profile_functions import  phone_fn,email_fn,name_fn,dob_fn,pincode_fn,address_fn
from func.common_functions import plot_profile_report,save_pdf
from pyspark.sql.functions import col, desc, row_number
import os
from pyspark.sql import Window


current_dir = os.getcwd()
global temp_dir
global function_dict
function_dict = {"Name":name_fn,"Phone":phone_fn,"Email":email_fn,"DOB":dob_fn,
                 "PINCODE":pincode_fn,"ADDRESS":address_fn}
temp_dir = current_dir+'/temp/'



def data_profiling(data,columns):
    total_cnt = data.count()
    global temp_dir
    global function_dict
    pie_img_li =  []
    # limit = columns['limit']
    # columns.pop('limit')    
    for i,j in list(function_dict.items()):
        for x,y in list(columns.items()):
            if x == i:
                print(j,i)
                data = data.withColumn(f'{y}_Conditions',j(col(y))) 
                df_pd = data.select(f'{y}_Conditions').toPandas()
                grouped_data_name = df_pd.groupby(f'{y}_Conditions').size().reset_index(name='Count')
                img = plot_profile_report(grouped_data_name,y,total_cnt)
                pie_img_li.append(img)

    pdf = save_pdf(pie_img_li,temp_dir)

    return {'Status':True,'data':data}





def groupBy_conditions(data, col_names, limit):
    
    result_data = {}

    for col_name in list(col_names.values()):


        # Perform groupBy on 'col_name' and get the count of occurrences for each value.
        general_groupby = data.select(col_name).groupBy(col_name).count().orderBy(desc('count')).limit(limit)

        # Create the condition column name by appending '_Conditions' to the input column name.
        cond_col_name = col_name + '_Conditions'

        # Get a list of unique conditions excluding 'N/A' and 'Valid'.
        uniq_li = [x[0] for x in data.select(cond_col_name).distinct().collect() if x[0] not in ['N/A', 'Valid']]
        new_df = data.groupBy(col_name, cond_col_name).count().orderBy(desc('count'))

        # Create a window specification partitioned by 'cond_col_name' and ordered by the count of occurrences.
        window_spec = Window.partitionBy(cond_col_name).orderBy(desc('count'))

        # Add a row number column to the DataFrame based on the window specification.
        df_with_row_number = new_df.select(col_name, cond_col_name, 'count').withColumn('row_num', row_number().over(window_spec))

        # Filter the DataFrame to get the first 'limit' records for each condition.
        condition_df = df_with_row_number.filter((col('row_num') <= limit) & (col(cond_col_name).isin(uniq_li)))

        # Drop the temporary 'row_num' column from the DataFrame.
        condition_df = condition_df.drop('row_num')

        # Convert the result DataFrame to a pandas DataFrame and store it in the result_data dictionary.
        result_data[col_name+'_Condition_GrpBy'] = condition_df.toPandas()
        result_data[col_name+'_General_GrpBy'] = general_groupby.toPandas()
        

    return result_data
