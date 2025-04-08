import polars as pl

from tasks.output import save_to_sqlite


def explode_titulaires(df: pl.DataFrame):
    # Explosion des champs titulaires sur plusieurs lignes (un titulaire de marché par ligne)
    # et une colonne par champ

    # Structure originale
    # [{{"id": "abc", "typeIdentifiant": "SIRET"}}]

    # Explosion de la liste de titulaires en autant de nouvelles lignes
    df = df.explode("titulaires")

    # Renommage du premier objet englobant
    df = df.select(
        pl.col("*"),
        pl.col("titulaires")
        .struct.rename_fields(["titulaires.object"])
        .alias("titulaires_renamed"),
    )

    # Extraction du premier objet dans une nouvelle colonne
    df = df.unnest("titulaires_renamed")

    # Renommage des champs de l'objet titulaire
    df = df.select(
        pl.col("*"),
        pl.col("titulaires.object")
        .struct.rename_fields(["titulaire.typeIdentifiant", "titulaire.id"])
        .alias("titulaire"),
    )

    # Extraction de l'objet titulaire
    df = df.unnest("titulaire")

    # Suppression des anciennes colonnes
    df = df.drop(["titulaires", "titulaires.object"])

    # Cast l'identifiant en string
    df = df.with_columns(pl.col("titulaire.id").cast(pl.String))

    return df


def normalize_tables(df):
    # MARCHES

    df_marches: pl.DataFrame = pl.DataFrame(df.to_arrow()).drop(
        "titulaire.id", "titulaire.typeIdentifiant"
    )
    df_marches = df_marches.unique("uid").sort(
        by="datePublicationDonnees", descending=True
    )
    save_to_sqlite(df_marches, "datalab", "marches", "uid")
    del df_marches

    # ACHETEURS

    df_acheteurs: pl.DataFrame = df.select("acheteur.id")
    df_acheteurs = df_acheteurs.rename({"acheteur.id": "id"})
    df_acheteurs = df_acheteurs.unique().sort(by="id")
    save_to_sqlite(df_acheteurs, "datalab", "acheteurs", "id")
    del df_acheteurs

    # TITULAIRES

    ## Table entreprises
    df_titulaires: pl.DataFrame = df.select("titulaire.id", "titulaire.typeIdentifiant")

    ### On garde les champs id et typeIdentifiant en clé primaire composite
    df_titulaires = df_titulaires.rename(
        {"titulaire.id": "id", "titulaire.typeIdentifiant": "typeIdentifiant"}
    )
    df_titulaires = df_titulaires.unique().sort(by=["id"])
    save_to_sqlite(df_titulaires, "datalab", "entreprises", "id, typeIdentifiant")
    del df_titulaires

    ## Table marches_titulaires
    df_marches_titulaires: pl.DataFrame = df.select(
        "uid", "titulaire.id", "titulaire.typeIdentifiant"
    )
    df_marches_titulaires = df_marches_titulaires.rename({"uid": "marche.uid"})
    save_to_sqlite(
        df_marches_titulaires,
        "datalab",
        "marches_titulaires",
        '"marche.uid", "titulaire.id", "titulaire.typeIdentifiant"',
    )
    del df_marches_titulaires

    # TODO ajouter les sous-traitants quand ils seront ajoutés aux données


def merge_decp_json(files: list) -> pl.DataFrame:
    dfs = []
    for file in files:
        df: pl.DataFrame = pl.read_parquet(f"{file}.parquet")
        dfs.append(df)

    df = pl.concat(dfs, how="diagonal")

    print("Suppression des lignes en doublon par UID (acheteur id + id)")
    # Exemple : 20005584600014157140791205100
    index_size_before = df.height
    df = df.unique(
        subset=["uid", "titulaire.id", "titulaire.typeIdentifiant"],
        maintain_order=False,
    )
    print("-- ", index_size_before - df.height, " doublons supprimés")

    # Ordre des colonnes
    df = df.select(
        "uid",
        "id",
        "nature",
        "acheteur.id",
        "titulaire.id",
        "titulaire.typeIdentifiant",
        "objet",
        "montant",
        "codeCPV",
        "procedure",
        "dureeMois",
        "dateNotification",
        "datePublicationDonnees",
        "formePrix",
        "attributionAvance",
        "offresRecues",
        "marcheInnovant",
        "ccag",
        "sousTraitanceDeclaree",
        "typeGroupementOperateurs",
        "tauxAvance",
        "origineUE",
        "origineFrance",
        "lieuExecution.code",
        "lieuExecution.typeCode",
        "idAccordCadre",
    )
    return df


#
# ⬇️⬇️⬇️ Fonctions à refactorer avec Polars et le format DECP 2022 ⬇️⬇️⬇️
#


def setup_tableschema_columns(df: pl.DataFrame):
    # Ajout colonnes manquantes

    df["donneesActuelles"] = ""  # TODO
    df["anomalies"] = ""  # TODO

    df_example_tableschema = pl.read_csv(
        "https://raw.githubusercontent.com/ColinMaudry/decp-table-schema/main/exemples/exemple-valide.csv",
        nrows=1,
    )
    df = df[df_example_tableschema.columns]

    return df


def make_decp_sans_titulaires(df: pl.DataFrame):
    df_decp_sans_titulaires = df.drop(
        columns=[
            "titulaire.id",
            "titulaire.typeIdentifiant",
        ]
    )
    df_decp_sans_titulaires = df_decp_sans_titulaires.drop_duplicates()
    return df_decp_sans_titulaires


def extract_unique_acheteurs_siret(df: pl.DataFrame):
    # Extraction des SIRET des DECP
    decp_acheteurs_df = df[["acheteur.id"]]
    decp_acheteurs_df = decp_acheteurs_df.drop_duplicates().loc[
        decp_acheteurs_df["acheteur.id"] != ""
    ]
    print(f"{decp_acheteurs_df.index.size} acheteurs uniques")

    return decp_acheteurs_df


def extract_unique_titulaires_siret(df: pl.DataFrame):
    # Extraction des SIRET des DECP
    df_sirets_titulaires = df[["titulaire.id", "titulaire.typeIdentifiant"]]

    df_sirets_titulaires = df_sirets_titulaires.drop_duplicates()
    df_sirets_titulaires = df_sirets_titulaires[
        df_sirets_titulaires["titulaire.typeIdentifiant"] == "SIRET"
    ]
    print(f"{len(df_sirets_titulaires)} titulaires uniques")

    return df_sirets_titulaires


def make_acheteur_nom(decp_acheteurs_df: pl.DataFrame):
    # Construction du champ acheteur.nom

    from numpy import nan as NaN

    def construct_nom(row):
        if row["enseigne1Etablissement"] is NaN:
            return row["denominationUniteLegale"]
        else:
            return f'{row["denominationUniteLegale"]} - {row["enseigne1Etablissement"]}'

    decp_acheteurs_df["acheteur.nom"] = decp_acheteurs_df.apply(construct_nom, axis=1)

    # TODO: ne garder que les colonnes acheteur.id et acheteur.nom

    return decp_acheteurs_df


def improve_titulaire_unite_legale_data(df_sirets_titulaires: pl.DataFrame):
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


def improve_categories_juridiques(df_sirets_titulaires: pl.DataFrame):
    # Récupération et raccourcissement des categories juridiques du fichier SIREN
    df_sirets_titulaires["categorieJuridiqueUniteLegale"] = (
        df_sirets_titulaires["categorieJuridiqueUniteLegale"].astype(str).str[:2]
    )

    # Récupération des libellés des catégories juridiques
    cj_df = pl.read_csv("data/cj.csv", index_col=None, dtype="object")
    df_sirets_titulaires = pl.merge(
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


def rename_titulaire_sirene_columns(df_sirets_titulaires: pl.DataFrame):
    # Renommage des colonnes

    renaming = {
        "activitePrincipaleEtablissement": "codeAPE",
        "etatAdministratifUniteLegale": "etatEntreprise",
        "etatAdministratifEtablissement": "etatEtablissement",
    }

    df_sirets_titulaires = df_sirets_titulaires.rename(columns=renaming)

    return df_sirets_titulaires
