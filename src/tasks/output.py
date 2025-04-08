import polars as pl
import os
import sqlite3


def save_to_files(df: pl.DataFrame, path: str):
    # Le format CSV ne supporte pas les données "nested", donc problématique pour les données "get"
    # df.write_csv(f"{path}.csv")
    df.write_parquet(f"{path}.parquet")


def save_to_sqlite(df: pl.DataFrame, database: str, table_name: str, primary_key: str):
    # Création de la table, avec les définitions de colonnes et de la ou des clés primaires
    column_definitions = []
    for column_name, column_type in zip(df.columns, df.dtypes):
        sql_type = "TEXT"  # Default
        if column_type in [pl.Int16, pl.Int64, pl.Boolean]:
            sql_type = "INTEGER"
        elif column_type in [pl.Float32, pl.Float64]:
            sql_type = "REAL"
        column_definitions.append(f'"{column_name}" {sql_type}')

    if "." in primary_key and not '"' in primary_key:
        raise ValueError(
            f"Les noms de colonnes contenant un point doivent être entre guillemets : {primary_key}"
        )

    primary_key_definition = (
        f"PRIMARY KEY({primary_key})"  # Peut être une clé composite. Ex : id, type
    )
    create_table_sql = f"CREATE TABLE \"{table_name}\" ({', '.join(column_definitions)}, {primary_key_definition})"  # Add quotes

    # Éxecution de la requête
    connection = sqlite3.connect(f"dist/{database}.sqlite")
    cursor = connection.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    cursor.execute(create_table_sql)
    connection.commit()
    connection.close()

    df.write_database(
        f'"{table_name}"', f"sqlite:///dist/{database}.sqlite", if_table_exists="append"
    )


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
