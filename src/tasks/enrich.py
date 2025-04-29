import polars as pl
from os import getenv
from config import SIRENE_DATA_DIR


def add_etablissement_data_to_acheteurs(df_siret_acheteurs: pl.DataFrame):
    etablissement_df_chunked = pl.read_csv(
        f"{SIRENE_DATA_DIR}/etablissements.parquet",
        chunksize=1000000,
        dtype="object",
        index_col=None,
        usecols=[
            "siret",
            "siren",
            # "denominationUsuelleEtablissement", vide
            "enseigne1Etablissement",
        ],
    )

    merged_chunks_list = []

    with etablissement_df_chunked as reader:
        for df_chunk in reader:
            merge = pl.merge(
                df_siret_acheteurs,
                df_chunk,
                how="inner",
                left_on="acheteur_id",
                right_on="siret",
            )
            if merge.index.size > 0:
                merged_chunks_list.append(merge)

    decp_acheteurs_df = pl.concat(merged_chunks_list).drop(columns=["siret"])

    del etablissement_df_chunked, df_chunk

    return decp_acheteurs_df


def add_etablissement_data(
    df: pl.LazyFrame, etablissement_columns: list, siret_column: str
) -> pl.LazyFrame:
    # Récupération des données SIRET titulaires
    schema_etablissements = {
        "siret": "object",
        "siren": "object",
        "longitude": "float",
        "latitude": "float",
        "activitePrincipaleEtablissement": "object",
        "codeCommuneEtablissement": "object",
        "etatAdministratifEtablissement": "category",
    }
    etablissement_df_chunked = pl.scan_csv(
        getenv(f"{SIRENE_DATA_DIR}/etablissements.parquet"),
        dtype=schema_etablissements,
        index_col=None,
        usecols=["siret"] + etablissement_columns,
    )

    df = pl.merge(
        df,
        etablissement_df_chunked,
        how="inner",
        left_on="titulaire_id",
        right_on="siret",
    )
    return df


def add_unite_legale_data(
    df: pl.LazyFrame, df_sirets_acheteurs: pl.LazyFrame, siret_column: str
) -> pl.LazyFrame:
    # Extraction du SIREN à partir du SIRET (9 premiers caractères)
    df_sirets_acheteurs = df_sirets_acheteurs.with_columns(
        pl.col(siret_column).str.head(9).alias("siren")
    )

    unites_legales_lf = pl.scan_parquet(SIRENE_DATA_DIR + "/unites_legales.parquet")

    # Pas besoin de garder les SIRET qui ne matchent pas dans ce df intermédiaire, puisqu'on
    # merge in fine avec le reste des données
    df_sirets_acheteurs = df_sirets_acheteurs.join(
        unites_legales_lf, how="inner", on="siren"
    )
    df_sirets_acheteurs = df_sirets_acheteurs.rename(
        {"denominationUniteLegale": "acheteur_nom", "siren": "acheteur_siren"}
    )

    # Ajout des données acheteurs enrichies au df de base
    df = df.join(df_sirets_acheteurs, how="left", on="acheteur_id")

    return df


def merge_sirets_acheteurs(decp_df: pl.DataFrame, df_sirets_acheteurs: pl.DataFrame):
    final_columns = ["acheteur_id", "acheteur_id"]

    decp_df = decp_df.drop(columns=["acheteur_id"])
    decp_df = pl.merge(
        decp_df,
        df_sirets_acheteurs[final_columns],
        on="acheteur_id",
        how="left",
    )

    del df_sirets_acheteurs

    return decp_df


def merge_sirets_titulaires(decp_df: pl.DataFrame, df_sirets_titulaires: pl.DataFrame):
    final_columns = [
        "id",
        "uid",
        "acheteur_id",
        "acheteur_id",
        "nature",
        "objet",
        "codeCPV",
        "lieuExecution_code",
        "lieuExecution_typeCode",
        "lieuExecution.nom",
        "dureeMois",
        "dateNotification",
        "montant",
        "titulaire_id",
        "titulaire_typeIdentifiant",
        "titulaire.denominationSociale",
        "codeAPE",
        "departement",
        "categorieEntreprise",  # plutôt que categorie
        "categorieJuridique",  # libellé
        # "categorieJuridiqueLibelle1",
        # "categorieJuridiqueLibelle2",
        "etatEtablissement",
        "etatEntreprise",
        "longitude",
        "latitude",
        "donneesActuelles",
        "anomalies",
    ]

    df_decp_titulaires = pl.merge(
        decp_df,
        df_sirets_titulaires,
        on=["titulaire_id", "titulaire_typeIdentifiant"],
        how="left",
    )
    df_decp_titulaires = df_decp_titulaires[final_columns]

    return df_decp_titulaires
