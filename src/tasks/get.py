import polars as pl
import pandas as pd
from httpx import get
import os
import json

from polars.polars import ColumnNotFoundError
from prefect import task
from prefect.futures import wait
from pathlib import Path

from tasks.output import save_to_sqlite, save_to_files


@task(retries=5, retry_delay_seconds=5)
def get_decp_json(json_files: dict, date_now: str) -> list:
    """Téléchargement des DECP publiées par Bercy sur data.gouv.fr."""
    return_files = []
    for json_file in json_files:
        print(json_file["file_name"], json_file["url"])
        if json_file["process"] is True:
            url = json_file["url"]
            file_name = json_file["file_name"]

            if url.startswith("https"):
                # Prod file
                decp_json_file: Path = Path(f"data/{file_name}_{date_now}.json")
            else:
                # Test file, pas de téléchargement
                decp_json_file: Path = Path(url)

            if not (os.path.exists(decp_json_file)):
                request = get(url, follow_redirects=True)
                with open(decp_json_file, "wb") as file:
                    file.write(request.content)
            else:
                print(f"DECP d'aujourd'hui déjà téléchargées ({date_now})")

            with open(decp_json_file) as f:
                decp_json = json.load(f)
            df: pl.DataFrame = pl.json_normalize(
                decp_json["marches"]["marche"],
                strict=False,
            )

            # Pour l'instant on ne garde pas les champs qui demandent une explosion
            # ou une eval
            # à part titulaires

            columns_to_drop = [
                "typesPrix.typePrix",
                "considerationsEnvironnementales.considerationEnvironnementale",
                "considerationsSociales.considerationSociale",
                "techniques.technique",
                "modalitesExecution.modaliteExecution",
                "modifications",
                "actesSousTraitance",
                "modificationsActesSousTraitance",
            ]

            for col in columns_to_drop:
                try:
                    df = df.drop(col)
                except ColumnNotFoundError:
                    pass

            file = f"dist/get/{file_name}_{date_now}"
            return_files.append(file)
            if not os.path.exists("dist/get"):
                os.mkdir("dist/get")
            save_to_files(df, file)
    return return_files


def get_stats():
    url = "https://www.data.gouv.fr/fr/datasets/r/8ded94de-3b80-4840-a5bb-7faad1c9c234"
    df_stats = pl.read_csv(url, index_col=None)
    return df_stats
