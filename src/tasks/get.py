import pandas as pd
from httpx import get
import os
from prefect import task
from pathlib import Path

from src.tasks.output import save_to_sqlite


def get_decp_csv(date_now: str, format: str):
    """Téléchargement des DECP publiées par Bercy sur data.economie.gouv.fr."""
    csv_url = os.getenv(f"DECP_ENRICHIES_VALIDES_{format}_URL")
    if csv_url.startswith("https"):
        # Prod file
        decp_augmente_valides_file: Path = Path(
            f"data/decp_augmente_valides_{format}_{date_now}.csv"
        )
    else:
        # Test file, pas de téléchargement
        decp_augmente_valides_file: Path = Path(csv_url)

    if not (os.path.exists(decp_augmente_valides_file)):
        request = get(csv_url)
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
def get_and_merge_decp_csv(date_now: str):
    # Pourrait être parallelisé
    df_2019: pd.DataFrame = get_decp_csv(date_now, "2019")
    df_2019 = df_2019.drop(
        columns=["TypePrix"]
    )  # SQlite le voit comme un doublon de typePrix, et les données semblent les mêmes
    save_to_sqlite(df_2019, "datalab", "data.economie.2019.ori")
    df_2019["source_open_data"] = "data.economie valides 2019"

    df_2022: pd.DataFrame = get_decp_csv(date_now, "2022")
    save_to_sqlite(df_2022, "datalab", "data.economie.2022.ori")
    df_2022["source_open_data"] = "data.economie valides 2022"

    # Suppression des colonnes abandonnées dans le format 2022
    df_2019 = df_2019.drop(
        columns=[
            "acheteur.nom",
            "lieuExecution.nom",
            "created_at",
            "updated_at",
            "titulaire_denominationSociale_1",
            "titulaire_denominationSociale_2",
            "titulaire_denominationSociale_3",
            "booleanModification",
            # Supprimés pour l'instant, possiblement réintégrées plus tard
            "actesSousTraitance",
            "titulairesModification",
            "modificationsActesSousTraitance",
            "objetModification",
        ]
    )

    # Renommage des colonnes qui ont changé de nom avec le format 2022
    df_2019.rename(
        columns={
            "technique": "techniques",
            "modaliteExecution": "modalitesExecution",
        }
    )

    # Concaténation des données format 2019 et 2022
    df = pd.concat([df_2019, df_2022], ignore_index=True)
    save_to_sqlite(df, "datalab", "data.economie.2019.2022")

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
