import pandas as pd

from cicd_etl import reading_file
from cicd_etl import transform_to_dataframe
from cicd_etl import drop_columns_with_missing_values
from cicd_etl import format_date_features
from cicd_etl import split_date_feature
#from cicd_etl import format_integer_features
#from cicd_etl import format_float_features
#from cicd_etl import format_string_features
#from cicd_etl import load_dataframe_in_db


def test_reading_file():
    expected_output = { "data" : [ [ "row-8eh8_xxkx-u3mq", "00000000-0000-0000-A1B7-70E47BCE5354",
                                     0, 1439382361, 1439382361, "83", "1" ]
        , [ "row-u2v5_78j5-pxk4", "00000000-0000-0000-260A-99DE31733069",
            0, 1439382361, 1439382361, "83", "1" ]
        , [ "row-68zj_7qfn-sxwu", "00000000-0000-0000-AA6F-0AA88BE0BC18",
            0, 1439382361, 1439382361, "83", "1" ]
        , [ "row-3yj2_u42c_mrn5", "00000000-0000-0000-0117-41785A45172B",
            0, 1439382361, 1439382361, "83", "1" ]
        , [ "row-eerp-uijb_4bdg", "00000000-0000-0000-7339-21CD63CCDFFA",
            0, 1439382361, 1439382361, "296", "0" ]]}
    input_file = "test_json.json"
    assert expected_output == reading_file(input_file)


def test_transform_to_dataframe():
    input_data = reading_file("rows.json")
    expected_datatype = "<class 'pandas.core.frame.DataFrame'>"
    assert expected_datatype == str(type(transform_to_dataframe(input_data)))


def test_columns_with_missing_values():
    input_dataframe = pd.DataFrame({'col1': [1, 2, 3, 4],
                                    'col2': [2, 0, None, 0],
                                    'col3': [7, 2, 1, 8]},
                                   index=['1', '2', '3', '4'])
    expected_dataframe = pd.DataFrame({'col1': [1, 2, 3, 4],
                                       'col3': [7, 2, 1, 8]},
                                      index=['1', '2', '3', '4'])
    expected_dataframe_shape = expected_dataframe.shape
    assert expected_dataframe_shape == drop_columns_with_missing_values(input_dataframe,10).shape

def test_format_date_features():
    input_dataframe = pd.DataFrame({'created_at': [1439382361]}, index=['1'])
    date_features = ["created_at"]
    expected_output ='           created_at\n1 2015-08-12 12:26:01'
    assert expected_output == str(format_date_features(input_dataframe,date_features))

def test_split_date_feature():
    date_features = ["created_at"]
    input_data_frame = pd.DataFrame({'created_at': ["2015-08-12"]}, index = ['1'])
    test_df = pd.DataFrame({'(day)': [12],
                            '(month)': [8],
                            '(year)': [2015]},
                           index=['1'])
    expected_shape = test_df.shape
    assert expected_shape == split_date_feature(input_data_frame,date_features).shape

#def test_format_integer_features():

#def test_format_float_features():

#def test_format_string_features():

#def test_load_dataframe_in_DB():
