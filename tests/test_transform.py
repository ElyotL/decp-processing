import pytest
import pandas as pd
import json_stream
from tasks.transform import identify_current_data


@pytest.fixture
def df_id_acheteurs() -> pd.DataFrame:
    df = pd.read_json("../tests/fixtures/decp_id_acheteurs.json")
    print(df)
    return df


@pytest.fixture
def decp_json():
    with open("../tests/fixtures/decp.json", "r") as file:
        return json_stream.load(file)


def test_identify_current_data(df_id_acheteurs, decp_json):
    result = identify_current_data(df_id_acheteurs, decp_json)
    print(result)
