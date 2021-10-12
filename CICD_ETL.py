# Import necessary python frameworks
import json
import pandas as pd
from sqlalchemy import create_engine

##################################################################################################
#################################### EXTRACT DATA ################################################
##################################################################################################
# Function to read/extract data
def reading_file(file):
    # open the json file
    file_open = open(file, encoding="utf-8")
    print("File opened!")
    # load the json file
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
    print("Data transformed and stored in a Dataframe!")
    return df_data


##################################################################################################
#################################### DATA MANIPULATION ###########################################
##################################################################################################

################################# MISSING VALUES MANAGEMENT ######################################
# Optional function to drop features with missing values that takes as input the dataframe and
# a threshold value from 0% to 100%
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

def drop_columns_with_missing_values(df, threshold):
    remaining_columns = columns_with_missing_values(df, threshold)
    non_nan_df = df[remaining_columns]
    non_nan_df = pd.DataFrame(non_nan_df)
    print("Shape of the reduced dataframe", non_nan_df.shape)
    return non_nan_df


################################ FORMATING DATE FEATURE ########################################
def format_date_features(data_frame, date_features):
    features = date_features
    for feature in features:
        data_frame[feature] = pd.to_datetime(data_frame[feature], unit="s")
    print("Date feature formated in human readable format.")
    return data_frame


################################ SPLITTING DATE FEATURE ########################################
def split_date_feature(data_frame, date_features):
    for feature in date_features:
        data_frame[feature] = pd.to_datetime(data_frame[feature])
        data_frame["day", feature] = data_frame[feature].dt.day
        data_frame["month", feature] = data_frame[feature].dt.month
        data_frame["year", feature] = data_frame[feature].dt.year
        data_frame = pd.DataFrame(data_frame)
        data_frame.drop([feature], inplace=True, axis=1)
    print("Date features split into day, month, year features.")
    return data_frame


#################### FEATURE DATA TYPE FORMATING (INTEGER, STRING, FLOAT)) #####################
# Integer
def format_integer_features(data_frame, int_features):
    for feature in int_features:
        data_frame = data_frame.astype({feature: int})
    print("Integer features formated in integer format.")
    return data_frame


# Float
def format_float_features(data_frame, float_features):
    for feature in float_features:
        data_frame = data_frame.astype({feature: float})
    print("Float features formated in float format.")
    return data_frame


# String
def format_string_features(data_frame, string_features):
    for feature in string_features:
        data_frame = data_frame.astype({feature: str})
    print("Textual features formated in string format.")
    return data_frame


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
    date_features = ["created_at", "updated_at"]
#    int_features = ["position", "MeasureId", "StateFips", "ReportYear", "MonitorOnly"]
    float_features = ["Value"]
    string_features = [
        "sid",
        "id",
        "MeasureName",
        "StratificationLevel",
        "StateName",
        "CountyName",
        "Unit",
        "UnitName",
        "DataOrigin",
    ]

    read_file = reading_file(file)
    transformed_data = transform_to_Dataframe(read_file)
    reduced_data_df = drop_columns_with_missing_values(transformed_data, 50)
    format_date = format_date_features(reduced_data_df, date_features)
    format_date = split_date_feature(format_date, date_features)
    # format_data = format_integer_features(format_date, int_features)
    format_data = format_float_features(format_date, float_features)
    format_data = format_string_features(format_data, string_features)
    load_dataframe_in_DB(format_data)
    print("Ingest process end!")


run_all_functions("rows.json")