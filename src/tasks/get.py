import pandas as pd
from httpx import get
import os
from prefect import task
from pathlib import Path


@task
def get_decp_csv(date_now: str):
    """Téléchargement des DECP publiées par Bercy sur data.economie.gouv.fr."""
    if os.getenv("DECP_ENRICHIES_VALIDES_2019_URL").startswith("https"):
        # Prod file
        decp_augmente_valides_file: Path = Path(
            f"data/decp_augmente_valides_{date_now}.csv"
        )
    else:
        # Test file, pas de téléchargement
        decp_augmente_valides_file: Path = Path(
            os.getenv("DECP_ENRICHIES_VALIDES_2019_URL")
        )

    if not (os.path.exists(decp_augmente_valides_file)):
        request = get(os.getenv("DECP_ENRICHIES_VALIDES_2019_URL"))
        with open(decp_augmente_valides_file, "wb") as file:
            file.write(request.content)
    else:
        print(f"DECP d'aujourd'hui déjà téléchargées ({date_now})")

    df: pd.DataFrame = pd.read_csv(
        decp_augmente_valides_file,
        sep=";",
        dtype=str,
        index_col=None,
    )

    return df


@task
def get_decp_json(date_now: str):
    import json_stream

    if os.getenv("DECP_JSON_URL").startswith("https"):
        # Prod file
        decp_json_file: Path = Path(f"data/decp_{date_now}.csv")
    else:
        # Test file, pas de téléchargement
        decp_json_file: Path = Path(os.getenv("DECP_JSON_URL"))

    if not (os.path.exists(decp_json_file)):
        request = get(os.getenv("DECP_JSON_URL"))
        with open(decp_json_file, "wb") as file:
            file.write(request.content)
    else:
        print(f"DECP JSON d'aujourd'hui déjà téléchargées ({date_now})")

    return json_stream.load(decp_json_file)


def get_stats():
    url = "https://www.data.gouv.fr/fr/datasets/r/8ded94de-3b80-4840-a5bb-7faad1c9c234"
    df_stats = pd.read_csv(url, index_col=None)
    return df_stats
