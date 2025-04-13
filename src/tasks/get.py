import polars as pl
from httpx import get
import os
import json

from polars.polars import ColumnNotFoundError
from prefect import task
from pathlib import Path

from tasks.output import save_to_files


@task(retries=5, retry_delay_seconds=5)
def get_json(date_now, json_file: dict):
    url = json_file["url"]
    filename = json_file["file_name"]

    if url.startswith("https"):
        # Prod file
        decp_json_file: Path = Path(f"data/{filename}_{date_now}.json")
        if not (os.path.exists(decp_json_file)):
            request = get(url, follow_redirects=True)
            with open(decp_json_file, "wb") as file:
                file.write(request.content)
        else:
            print(f"[{filename}] DECP d'aujourd'hui déjà téléchargées ({date_now})")
    else:
        # Test file, pas de téléchargement
        decp_json_file: Path = Path(url)

    return decp_json_file


@task
def get_decp_json(json_files: dict, date_now: str) -> list:
    """Téléchargement des DECP publiées par Bercy sur data.gouv.fr."""
    return_files = []
    for json_file in json_files:
        if json_file["process"] is True:
            decp_json_file: Path = get_json(date_now, json_file)

            with open(decp_json_file) as f:
                decp_json = json.load(f)

            filename = json_file["file_name"]
            path = decp_json["marches"]["marche"]
            df: pl.DataFrame = pl.json_normalize(
                path,
                strict=False,
                infer_schema_length=10000,
                separator="_",
                # Remplacement des "." dans les noms de colonnes par des "_" car
                # en SQL ça oblige à entourer les noms de colonnes de guillemets
            )

            df = df.with_columns(
                pl.lit(f"data.gouv.fr {filename}.json").alias("source_open_data")
            )

            # Pour l'instant on ne garde pas les champs qui demandent une explosion
            # ou une eval à part titulaires

            columns_to_drop = [
                # Pas encore incluses
                "typesPrix_typePrix",
                "considerationsEnvironnementales_considerationEnvironnementale",
                "considerationsSociales_considerationSociale",
                "techniques_technique",
                "modalitesExecution.modaliteExecution",
                "modifications",
                "actesSousTraitance",
                "modificationsActesSousTraitance",
                # Champs de concessions
                "_type",  # Marché ou Contrat de concession
                "autoriteConcedante",
                "concessionnaires",
                "donneesExecution",
                "valeurGlobale",
                "montantSubventionPublique",
                "dateSignature",
                "dateDebutExecution",
                # Champs ajoutés par e-marchespublics (decp-2022)
                "offresRecues_source",
                "marcheInnovant_source",
                "attributionAvance_source",
                "sousTraitanceDeclaree_source",
                "dureeMois_source",
            ]

            for col in columns_to_drop:
                try:
                    df = df.drop(col)
                except ColumnNotFoundError:
                    pass

            print(f"[{filename}]", df.shape)

            file = f"dist/get/{filename}_{date_now}"
            if not os.path.exists("dist/get"):
                os.mkdir("dist/get")
            save_to_files(df, file)

            return_files.append(file)

    return return_files


def get_stats():
    url = "https://www.data.gouv.fr/fr/datasets/r/8ded94de-3b80-4840-a5bb-7faad1c9c234"
    df_stats = pl.read_csv(url, index_col=None)
    return df_stats
