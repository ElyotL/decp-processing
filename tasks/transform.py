import pandas as pd


def explode_titulaires(df: pd.DataFrame):
    # VERS LE FORMAT DECP-TABLE-SCHEMA #

    # Explosion des champs titulaires sur plusieurs lignes (un titulaire de marché par ligne)

    df["titulaire.id"] = [[] for r in range(len(df))]
    df["titulaire.denominationSociale"] = [[] for r in range(len(df))]
    df["titulaire.typeIdentifiant"] = [[] for r in range(len(df))]

    for num in range(1, 4):
        mask = df[f"titulaire_id_{num}"] != ""
        df.loc[mask, "titulaire.id"] += df.loc[mask, f"titulaire_id_{num}"].apply(
            lambda x: [x]
        )
        df.loc[mask, "titulaire.denominationSociale"] += df.loc[
            mask, f"titulaire_denominationSociale_{num}"
        ].apply(lambda x: [x])
        df.loc[mask, "titulaire.typeIdentifiant"] += df.loc[
            mask, f"titulaire_typeIdentifiant_{num}"
        ].apply(lambda x: [x])

    df = df.explode(
        ["titulaire.id", "titulaire.denominationSociale", "titulaire.typeIdentifiant"],
        ignore_index=True,
    )

    return df


def add_missing_columns(df: pd.DataFrame):
    # Ajout colonnes manquantes

    df["uid"] = df["acheteur.id"] + df["id"]
    df["donneesActuelles"] = ""  # TODO
    df["anomalies"] = ""  # TODO

    return df


def make_decp_sans_titulaires(df: pd.DataFrame):
    df_decp_sans_titulaires = df.drop(
        columns=["titulaire.id", "titulaire.nom", "titulaire.typeIdentifiant"]
    )
    df_decp_sans_titulaires = df_decp_sans_titulaires.drop_duplicates()
    return df_decp_sans_titulaires


def extract_unique_acheteurs_siret(df: pd.DataFrame):
    # Extraction des SIRET des DECP
    decp_acheteurs_df = df[["acheteur.id"]].copy()
    decp_acheteurs_df = decp_acheteurs_df.drop_duplicates().loc[
        decp_acheteurs_df["acheteur.id"] != ""
    ]
    print(f"{decp_acheteurs_df.index.size} acheteurs uniques")

    return decp_acheteurs_df


def extract_unique_titulaires_siret(df: pd.DataFrame):
    # Extraction des SIRET des DECP
    df_sirets_titulaires = df[["titulaire.id", "titulaire.typeIdentifiant"]]

    df_sirets_titulaires = df_sirets_titulaires.drop_duplicates()
    df_sirets_titulaires = df_sirets_titulaires[
        df_sirets_titulaires["titulaire.typeIdentifiant"] == "SIRET"
    ]
    print(f"{df_sirets_titulaires.index.size} titulaires uniques")

    return df_sirets_titulaires


def make_acheteur_nom(decp_acheteurs_df: pd.DataFrame):
    # Construction du champ acheteur.nom

    from numpy import NaN

    def construct_nom(row):
        if row["enseigne1Etablissement"] is NaN:
            return row["denominationUniteLegale"]
        else:
            return f'{row["denominationUniteLegale"]} - {row["enseigne1Etablissement"]}'

    decp_acheteurs_df["acheteur.nom"] = decp_acheteurs_df.apply(construct_nom, axis=1)

    # TODO: ne garder que les colonnes acheteur.id et acheteur.nom

    return decp_acheteurs_df


def improve_titulaire_unite_legale_data(df_sirets_titulaires: pd.DataFrame):
    # Raccourcissement du code commune
    df_sirets_titulaires["departement"] = df_sirets_titulaires[
        "codeCommuneEtablissement"
    ].str[:2]
    df_sirets_titulaires = df_sirets_titulaires.drop(
        columns=["codeCommuneEtablissement"]
    )

    # # Raccourcissement de l'activité principale
    # pas sûr de pourquoi je voulais raccourcir le code NAF/APE. Pour récupérérer des libellés ?
    # decp_titulaires_sirets_df['activitePrincipaleEtablissement'] = decp_titulaires_sirets_df['activitePrincipaleEtablissement'].str[:-3]

    # Correction des données ESS et état
    df_sirets_titulaires["etatAdministratifUniteLegale"] = df_sirets_titulaires[
        "etatAdministratifUniteLegale"
    ].cat.rename_categories({"A": "Active", "C": "Cessée"})
    df_sirets_titulaires["economieSocialeSolidaireUniteLegale"] = df_sirets_titulaires[
        "economieSocialeSolidaireUniteLegale"
    ].replace({"O": "Oui", "N": "Non"})

    df_sirets_titulaires = improve_categories_juridiques(df_sirets_titulaires)

    return df_sirets_titulaires


def improve_categories_juridiques(df_sirets_titulaires: pd.DataFrame):
    # Récupération et raccourcissement des categories juridiques du fichier SIREN
    df_sirets_titulaires["categorieJuridiqueUniteLegale"] = (
        df_sirets_titulaires["categorieJuridiqueUniteLegale"].astype(str).str[:2]
    )

    # Récupération des libellés des catégories juridiques
    cj_df = pd.read_csv("data/cj.csv", index_col=None, dtype="object")
    df_sirets_titulaires = pd.merge(
        df_sirets_titulaires,
        cj_df,
        how="left",
        left_on="categorieJuridiqueUniteLegale",
        right_on="Code",
    )
    df_sirets_titulaires["categorieJuridique"] = df_sirets_titulaires["Libellé"]
    df_sirets_titulaires = df_sirets_titulaires.drop(
        columns=["Code", "categorieJuridiqueUniteLegale", "Libellé"]
    )
    return df_sirets_titulaires


def rename_titulaire_sirene_columns(df_sirets_titulaires: pd.DataFrame):
    # Renommage des colonnes

    renaming = {
        "activitePrincipaleEtablissement": "codeAPE",
        "etatAdministratifUniteLegale": "etatEntreprise",
        "etatAdministratifEtablissement": "etatEtablissement",
    }

    df_sirets_titulaires = df_sirets_titulaires.rename(columns=renaming)

    return df_sirets_titulaires
