# from pyspark.sql.functions import Row
import matplotlib.pyplot as plt
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType
import numpy as np
import streamlit as st
from PIL import Image
import os
import matplotlib.pyplot as plt
from io import BytesIO
from matplotlib.backends.backend_pdf import PdfPages


import configparser
config = configparser.ConfigParser()
config.read("configration.properties")


cwd = os.getcwd()

def warning_message_with_icon(message):
    st.markdown(
        f"""
        <div style="display: flex; align-items: center;">
            <span style="font-size: 1.5em; color: red; margin-right: 0.5em;">&#9888;</span>
            <span style="font-weight: bold; color: red;">{message}</span>
        </div>
        """,
        unsafe_allow_html=True
    )
    
def img(im_path,size=(400,300)):
  image = Image.open(im_path)
  new_size = size  
  resized_image = image.resize(new_size)
  return resized_image

warning_image = img(cwd+'/images/warning.jpg')


def get_properties():
    # Convert properties to a dictionary
    properties_dict = {}
    for section in config.sections():
        properties_dict[section] = {}
        for key, value in config.items(section):
            properties_dict[section][key] = value

    return properties_dict


def save_properties(ip_props,name):
    config[name]      = ip_props
    with open("configration.properties", "w") as configfile:
        config.write(configfile)
    return True



def auto_id(data, Spark_Session):
    # Convert DataFrame to RDD and add index
    rdd_data = data.rdd.zipWithIndex()
    # Convert RDD with index back to DataFrame
    data_with_index = rdd_data.map(lambda row: Row(**row[0].asDict(), psx_id=row[1] + 1))

    # Define the schema for the new DataFrame
    schema = StructType(data.schema.fields + [StructField("psx_id", IntegerType(), False)])

    # Create the DataFrame with the specified schema
    data = Spark_Session.createDataFrame(data_with_index, schema=schema)
    return data




def func(pct, allvals):
    absolute = int(np.round(pct / 100. * np.sum(allvals)))
    return f"{pct:.1f}%\n({absolute:d})"

def plot_profile_report(data, title,total_count):
    df = data
    print(total_count)
    print('*'*100)


    # Filter out rows with count value of 0
    df = df[df[df.columns[1]] != 0]
    fig, ax = plt.subplots(figsize=(16, 10), subplot_kw=dict(aspect="equal"))

    counts = df[df.columns[1]].to_list()
    names = df[df.columns[0]].to_list()

    wedges,  autotexts = ax.pie(counts, 
                                      textprops=dict(color="w"))

    ax.legend(wedges, names,
              title="Parameters",
              loc="center left",
              bbox_to_anchor=(1, 0, 0.5, 1),
              fontsize=16)

    plt.setp(autotexts, size=14, weight="bold")
    ax.set_title(f"Profiling Summary Report - {title}", fontsize=28, fontweight='bold', fontfamily='Arial', color='green', pad=15)

    # Sort table_data in descending order based on counts
    sorted_indices = np.argsort(counts)[::-1]
    sorted_names = [names[i] for i in sorted_indices]
    sorted_counts = [counts[i] for i in sorted_indices]
    table_data = [[name, f"{count} ({count / sum(counts) * 100:.1f}%)]"] for name, count in zip(sorted_names, sorted_counts)]
    table_data.append(["Total Count", f"{total_count}"])

    table = ax.table(cellText=table_data, colLabels=["Parameters", "Count (%)"],
                     loc="bottom", cellLoc="center", colColours=["lightgray"] * 2)
    


    table.auto_set_font_size(False)
    table.set_fontsize(12)

    # Hide the axes
    ax.axis("off")

    # Convert the plot to an image
    buf = BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format="png")
    buf.seek(0)
    
    # Close the plot to free up resources
    plt.close()

    return buf


def save_pdf(image,path):

    # Create a PdfPages object to save the images in the PDF
    with PdfPages(path+'profiling_report.pdf') as pdf:
        for im in image:
            # Load the image using PIL
            image = Image.open(im)

            # Create a figure to plot the image
            fig = plt.figure(figsize=(24, 18))

            # Plot the image on the figure
            plt.imshow(image)

            # Remove axes and labels (optional)
            plt.axis('off')

            # Set a title (optional)

            # Save the figure to the PDF
            # pdf.savefig(fig)
            pdf.savefig(fig, bbox_inches='tight')

            # Close the figure to release memory
            plt.close(fig)

    # The PDF is automatically closed after exiting the 'with' block





def remove_files_from_folder(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)

def clear_property_file(file_path):
    try:
        with open(file_path, 'w') as file:
            file.write('')  # Writing an empty dictionary to clear the file content
        print(f"Property file '{file_path}' cleared successfully.")
    except Exception as e:
        print(f"Failed to clear the property file '{file_path}'. Error: {str(e)}")

