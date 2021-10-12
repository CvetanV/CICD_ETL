import pandas as pd

from CICD_ETL import reading_file
from CICD_ETL import transform_to_Dataframe
from CICD_ETL import drop_columns_with_missing_values
from CICD_ETL import format_date_features
from CICD_ETL import split_date_feature
from CICD_ETL import format_integer_features
from CICD_ETL import format_float_features
from CICD_ETL import format_string_features
from CICD_ETL import load_dataframe_in_DB


def test_reading_file():
    expected_output = { "data" : [ [ "row-8eh8_xxkx-u3mq", "00000000-0000-0000-A1B7-70E47BCE5354", 0, 1439382361, 1439382361, "83", "1" ]
        , [ "row-u2v5_78j5-pxk4", "00000000-0000-0000-260A-99DE31733069", 0, 1439382361, 1439382361, "83", "1" ]
        , [ "row-68zj_7qfn-sxwu", "00000000-0000-0000-AA6F-0AA88BE0BC18", 0, 1439382361, 1439382361, "83", "1" ]
        , [ "row-3yj2_u42c_mrn5", "00000000-0000-0000-0117-41785A45172B", 0, 1439382361, 1439382361, "83", "1" ]
        , [ "row-eerp-uijb_4bdg", "00000000-0000-0000-7339-21CD63CCDFFA", 0, 1439382361, 1439382361, "296", "0" ]]}
    input_file = "test_json.json"
    assert expected_output == reading_file(input_file)


def test_transform_to_Dataframe():
    input_data = reading_file("rows.json")
    expected_datatype = "<class 'pandas.core.frame.DataFrame'>"
    assert expected_datatype == str(type(transform_to_Dataframe(input_data)))


#def test_columns_with_missing_values():
#    input_dataframe = pd.DataFrame({'col1': [1, 2, 3, 4],
#                                    'col2': [2, 0, None, 0],
#                                    'col3': [7, 2, 1, 8]},
#                                   index=['1', '2', '3', '4'])
#    expected_dataframe = pd.DataFrame({'col1': [1, 2, 3, 4],
#                                       'col3': [7, 2, 1, 8]},
#                                      index=['1', '2', '3', '4'])
#    assert expected_dataframe == drop_columns_with_missing_values(input_dataframe,10)

#def test_format_date_features():
#    date_features = ["created_at"]
#    input_df = reading_file("rows.json")
#    input_df = transform_to_Dataframe(input_df)
#    #input_data = input_df[:1]
#    expected_data_frame = pd.DataFrame({'created_at': ["2015-08-12 12:26:01"]}, index = ['0'])
#    expected_data = expected_data_frame['created_at'][:1]
#    assert expected_data == format_date_features(input_df,date_features)[:1]

#def test_split_date_feature():

#def test_format_integer_features():

#def test_format_float_features():

#def test_format_string_features():

#def test_load_dataframe_in_DB():