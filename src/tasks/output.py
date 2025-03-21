import polars as pl
import os
from sqlalchemy import create_engine


def save_to_files(df: pl.DataFrame, path: str):
    df.to_csv(f"{path}.csv", index=None)
    df.to_parquet(f"{path}.parquet", index=None)


def save_to_sqlite(df: pl.DataFrame, database: str, table_name: str):
    conn = create_engine(f"sqlite:///dist/{database}.sqlite", echo=False)
    df.write_database(table_name, conn, if_table_exists="replace")


def make_data_package():
    from frictionless import Package, Resource, Pipeline, steps

    common_steps = [
        steps.field_update(name="id", descriptor={"type": "string"}),
        steps.field_update(name="uid", descriptor={"type": "string"}),
        steps.field_update(name="acheteur.id", descriptor={"type": "string"}),
        steps.field_update(name="acheteur.nom", descriptor={"type": "string"}),
    ]

    outputs = [
        {
            "csv": "decp.csv",
            "steps": common_steps
            + [
                steps.field_update(name="titulaire.id", descriptor={"type": "string"}),
            ],
        },
        # {
        #     "csv": "decp-sans-titulaires.csv",
        #     "steps": common_steps,
        # },
        # {
        #     "csv": "decp-titulaires.csv",
        #     "steps": common_steps
        #     + [
        #         steps.field_update(name="departement", descriptor={"type": "string"}),
        #         steps.field_update(name="titulaire.id", descriptor={"type": "string"}),
        #     ],
        # },
    ]

    resources = []

    for output in outputs:
        resource: Resource = Resource(path=output["csv"])

        # Cette méthode détecte les caractéristiques du CSV et tente de deviner les datatypes
        resource.infer()
        resource = resource.transform(Pipeline(steps=output["steps"]))
        resources.append(resource)

    Package(
        name="decp",
        title="DECP tabulaire",
        description="Données essentielles de la commande publique (FR) au format tabulaire.",
        resources=resources,
        # it's possible to provide all the official properties like homepage, version, etc
    ).to_json("datapackage.json")


def make_sqllite_and_datasette_metadata():
    from datapackage_to_datasette import datapackage_to_datasette

    if os.path.exists("decp.sqlite"):
        os.remove("decp.sqlite")

    datapackage_to_datasette(
        dbname="decp.sqlite",
        data_package="datapackage.json",
        metadata_filename="datasette_metadata.json",
        write_mode="replace",
    )
