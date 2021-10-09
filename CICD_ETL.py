# Import necessary python frameworks
import json
import pandas as pd
from sqlalchemy import create_engine

##################################################################################################
#################################### EXTRACT DATA ################################################
##################################################################################################
# Function to read/extract, transform and load the data from a json file into a relational database
def reading_file(file):
    file_open = open(file, encoding="utf-8")
    print("File opened!")
    read_file = json.load(file_open)
    print("File loaded!")
    # close the json file
    file_open.close()
    print("File closed!")
    return read_file


##################################################################################################
#################################### TRANSFORM DATA ##############################################
##################################################################################################
def transform_to_Dataframe(read_file):
    # declare empty metadata list
    metadata = []
    reading_metadata = read_file["meta"]["view"]["columns"]
    for row in reading_metadata:
        metadata.append(row)  # populate the empty metadata list

    # transform list to pandas dataframe
    df_metadata = pd.DataFrame(metadata)

    # extract the names of the columns
    dataframe_columns = df_metadata.name

    # store the names of the columns in a list
    columns = list(dataframe_columns)
    print("Extracted names of columns!")

    # declare empty data list
    data = []
    read_data = read_file["data"]
    for d in read_data:
        data.append(d)  # populate the empty data list

    # transform list to pandas dataframe
    df_data = pd.DataFrame(data)
    print("Extracted data from file!")

    # replace the names of the columns of the dataframe with
    # the names of the columns stored in the columns variable
    df_data.columns = columns

    return df_data


##################################################################################################
#################################### DATA MANIPULATION ###########################################
##################################################################################################
# Function to check missing values in the dataframe
def columns_with_missing_values(dataf, threshold):
    l = []
    l = list(
        dataf.drop(
            dataf.loc[
                :, list((100 * (dataf.isnull().sum() / len(dataf.index)) >= threshold))
            ].columns,
            1,
        ).columns.values
    )
    print(
        "Number of columns having %s percent or more missing values: " % threshold,
        (dataf.shape[1] - len(l)),
    )
    print("Dropped columns:", list(set(list((dataf.columns.values))) - set(l)))
    return l


######### MISSING VALUES MANAGEMENT
# Optional function to drop features with missing values
# that takes as input the dataframe and a threshold value
# from 0% to 100%
"""
features = columns_with_missing_values(df, 50)
df100 = df[features]
print("Transformed Data!")
"""
########## DATA FORMATING (DATE, TIME, OTHER COLUMNS(INTEGER, STRING, FLOAT, OBJECT))


##################################################################################################
#################################### LOAD DATA ###################################################
##################################################################################################
def load_dataframe_in_DB(df_data):
    print("Starting loading data into a postgre database...")
    engine = create_engine("postgresql://postgres:pass@localhost:5432/postgres")
    df_data.to_sql("docebo_ex_tab", engine, if_exists="append")
    print("Data loaded into a postgre database!")


##################################################################################################
#################################### RUN FUNCTIONS ###############################################
##################################################################################################
def run_all_functions(file):
    read_file = reading_file(file)
    transformed_data = transform_to_Dataframe(read_file)
    load_dataframe_in_DB(transformed_data)


run_all_functions("rows.json")
