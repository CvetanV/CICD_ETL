# Import necessary python frameworks
import pandas as pd
import json
from sqlalchemy import create_engine

# Function to check missing values in the dataframe
def columns_with_missing_values(df, threshold):
    l = []
    l = list(df.drop(df.loc[:,list((100*(df.isnull().sum()/len(df.index)) >= threshold))].columns, 1).columns.values)
    print("Number of columns having %s percent or more missing values: "%threshold, (df.shape[1] - len(l)))
    print("Dropped columns:", list(set(list((df.columns.values))) - set(l)))
    return l

# Function to read/extract, transform and load the data from a json file into a relational database
def ingest_data(file):
    file_open = open(file)
    print("File opened!")
    read_file = json.load(file_open)
    print("File loaded!")

    #declare empty metadata list
    metadata = []
    reading_metadata = read_file["meta"]["view"]["columns"]
    for row in reading_metadata:
        metadata.append(row) #populate the empty metadata list

    #transform list to pandas dataframe
    df_metadata = pd.DataFrame(metadata)

    #extract the names of the columns
    dataframe_columns = df_metadata.name

    #store the names of the columns in a list
    columns = list(dataframe_columns)
    print("Extracted names of columns!")

    #declare empty data list
    data = []
    read_data = read_file["data"]
    for d in read_data:
        data.append(d) #populate the empty data list

    #transform list to pandas dataframe
    df_data = pd.DataFrame(data)
    print("Extracted data from file!")

    #close the json file
    file_open.close()
    print("File closed!")

    #replace the names of the columns of the dataframe with
    #the names of the columns stored in the columns variable
    df_data.columns = columns

    # Optional function to drop features with missing values
    # that takes as input the dataframe and a threshold value
    # from 0% to 100%
    """
    features = columns_with_missing_values(df, 50)
    df100 = df[features]
    print("Transformed Data!")
    """
    print("Starting loading data into a postgre database...")
    engine = create_engine('postgresql://postgres:pass@localhost:5432/postgres')
    df_data.to_sql('docebo_ex_tab', engine, if_exists='append')
    print("Data loaded into a postgre database!")