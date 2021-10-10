from CICD_ETL import reading_file
from CICD_ETL import transform_to_Dataframe
from CICD_ETL import columns_with_missing_values
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


#def test_load_dataframe_in_DB():