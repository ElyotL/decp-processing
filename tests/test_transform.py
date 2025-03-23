import pytest
import polars as pl
from tasks.transform import identify_current_data


@pytest.fixture
def df_id_acheteurs() -> pl.DataFrame:
    data = [
        {"id": "20202020F1234900", "acheteur.id": "13000548100010"},
        {"id": "20202020F1234901", "acheteur.id": "13000548100010"},
    ]

    return pl.DataFrame(data)


@pytest.fixture
def decp_json():
    data = {
        "marches": {
            "marche": [
                {
                    "modifications": [],
                    "nature": "ACCORD-CADRE",
                    "datePublicationDonnees": "2020-08-06",
                    "id": "20202020F1234900",
                    "acheteur": {"id": "13000548100010"},
                },
                {
                    "modifications": [{}],
                    "nature": "ACCORD-CADRE",
                    "datePublicationDonnees": "2020-08-09",
                    "id": "20202020F1234901",
                    "acheteur": {"id": "13000548100010"},
                },
                {
                    "modifications": [{}],
                    "datePublicationDonnees": "2017-01-03",
                    "id": "20172019S0767300",
                    "acheteur": {"id": "26380030200014"},
                },
                {
                    "datePublicationDonnees": "2019-01-03",
                    "id": "20192019S0767300",
                    "acheteur": {"id": "26380030200014"},
                },
            ]
        }
    }
    return pl.DataFrame(data)


def test_identify_current_data(df_id_acheteurs, decp_json):
    # On ne teste pas cette feature pour l'instant
    # result = identify_current_data(df_id_acheteurs, decp_json)
    # print(result)
    pass
