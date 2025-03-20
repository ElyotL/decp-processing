import pandas as pd
from os import getenv


def add_etablissement_data_to_acheteurs(df_siret_acheteurs: pd.DataFrame):
    etablissement_df_chunked = pd.read_csv(
        getenv("SIRENE_ETABLISSEMENTS_PATH"),
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
            merge = pd.merge(
                df_siret_acheteurs,
                df_chunk,
                how="inner",
                left_on="acheteur.id",
                right_on="siret",
            )
            if merge.index.size > 0:
                merged_chunks_list.append(merge)

    decp_acheteurs_df = pd.concat(merged_chunks_list).drop(columns=["siret"])

    del etablissement_df_chunked, df_chunk

    return decp_acheteurs_df


def add_unite_legale_data_to_acheteurs(decp_acheteurs_df: pd.DataFrame):
    unite_legale_df_chunked = pd.read_csv(
        getenv("SIRENE_UNITES_LEGALES_PATH"),
        index_col=None,
        dtype="object",
        sep=",",
        chunksize=1000000,
        usecols=[
            "siren",
            "denominationUniteLegale",
            # "sigleUniteLegale" trop variable, parfois long
        ],
    )

    merged_chunks_list = []

    with unite_legale_df_chunked as reader:
        for df_chunk in reader:
            merge = pd.merge(decp_acheteurs_df, df_chunk, how="inner", on="siren")
            if not merge.empty and merge.notnull().any().any() and len(merge) >= 1:
                merged_chunks_list.append(merge)
    del unite_legale_df_chunked, df_chunk

    decp_acheteurs_df = pd.concat(merged_chunks_list)

    del merged_chunks_list

    return decp_acheteurs_df


def add_etablissement_data_to_titulaires(df_sirets_titulaires: pd.DataFrame):
    # Récupération des données SIRET titulaires
    dtypes_etablissements = {
        "siret": "object",
        "siren": "object",
        "longitude": "float",
        "latitude": "float",
        "activitePrincipaleEtablissement": "object",
        "codeCommuneEtablissement": "object",
        "etatAdministratifEtablissement": "category",
    }
    etablissement_df_chunked = pd.read_csv(
        getenv("SIRENE_ETABLISSEMENTS_PATH"),
        chunksize=(500000),
        dtype=dtypes_etablissements,
        index_col=None,
        usecols=[
            "siret",
            "siren",
            "longitude",
            "latitude",
            "activitePrincipaleEtablissement",
            "codeCommuneEtablissement",
            "etatAdministratifEtablissement",
        ],
    )

    merged_chunks_list = []

    # 6 min 20 en gardant tous les merges, 10 000 chunks. Resultat 38M rows!
    # 6 min 27 avec inner merge et en ne gardant que les merge non vides
    # 4 min 04 avec 200 000 chunks
    # 3 min 27 avec 500 000 chunks
    # 3 min 05 avec 1M chunks

    with etablissement_df_chunked as reader:
        for df_chunk in reader:
            merge = pd.merge(
                df_sirets_titulaires,
                df_chunk,
                how="inner",
                left_on="titulaire.id",
                right_on="siret",
            )
            if merge.index.size > 0:
                merged_chunks_list.append(merge)

    df_sirets_titulaires = pd.concat(merged_chunks_list).drop(columns=["siret"])

    del etablissement_df_chunked, df_chunk

    return df_sirets_titulaires


def add_unite_legale_data_to_titulaires(df_sirets_titulaires: pd.DataFrame):
    dtypes_uniteLegales = {
        "siren": "object",
        "categorieEntreprise": "object",  # doit être object, car il y a des NaN
        "etatAdministratifUniteLegale": "category",
        "economieSocialeSolidaireUniteLegale": "object",  # doit être object, car il y a des NaN
        "categorieJuridiqueUniteLegale": "object",  # object plutôt que catégorie pour faire des modifications plus tard
    }

    unite_legale_df_chunked = pd.read_csv(
        getenv("SIRENE_UNITES_LEGALES_PATH"),
        index_col=None,
        dtype=dtypes_uniteLegales,
        sep=",",
        chunksize=500000,
        usecols=[
            "siren",
            "categorieEntreprise",
            "etatAdministratifUniteLegale",
            "economieSocialeSolidaireUniteLegale",
            "categorieJuridiqueUniteLegale",
        ],
    )

    merged_chunks_list = []

    with unite_legale_df_chunked as reader:
        for df_chunk in reader:
            merge = pd.merge(df_sirets_titulaires, df_chunk, how="inner", on="siren")
            if not merge.empty and merge.notnull().any().any() and len(merge) >= 1:
                merged_chunks_list.append(merge)
    del unite_legale_df_chunked, df_chunk

    df_sirets_titulaires = pd.concat(merged_chunks_list)

    del merged_chunks_list

    return df_sirets_titulaires


def merge_sirets_acheteurs(decp_df: pd.DataFrame, df_sirets_acheteurs: pd.DataFrame):
    final_columns = ["acheteur.id", "acheteur.nom"]

    decp_df = decp_df.drop(columns=["acheteur.nom"])
    decp_df = pd.merge(
        decp_df,
        df_sirets_acheteurs[final_columns],
        on="acheteur.id",
        how="left",
    )

    del df_sirets_acheteurs

    return decp_df


def merge_sirets_titulaires(decp_df: pd.DataFrame, df_sirets_titulaires: pd.DataFrame):
    final_columns = [
        "id",
        "uid",
        "acheteur.id",
        "acheteur.nom",
        "nature",
        "objet",
        "codeCPV",
        "lieuExecution.code",
        "lieuExecution.typeCode",
        "lieuExecution.nom",
        "dureeMois",
        "dateNotification",
        "montant",
        "titulaire.id",
        "titulaire.typeIdentifiant",
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

    df_decp_titulaires = pd.merge(
        decp_df,
        df_sirets_titulaires,
        on=["titulaire.id", "titulaire.typeIdentifiant"],
        how="left",
    )
    df_decp_titulaires = df_decp_titulaires[final_columns]

    return df_decp_titulaires
