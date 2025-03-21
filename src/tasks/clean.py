import polars as pl
from numpy import nan


def clean_official_decp(df: pl.DataFrame):
    df = df.with_columns(pl.col(pl.String).str.replace_many([nan, None], ""))

    # Nettoyage des identifiants de marchés
    df = df.with_columns(pl.col("id").str.replace_all(r"[,\\./]", "_"))

    # Ajout du champ uid
    # TODO: à déplacer autre part, dans transform
    df = df.with_columns((pl.col("acheteur.id") + pl.col("id")).alias("uid"))

    # Suppression des lignes en doublon par UID (acheteur id + id)
    # Exemple : 20005584600014157140791205100
    index_size_before = df.height
    df = df.unique(subset=["uid"], maintain_order=False)
    print("-- ", index_size_before - df.height, " doublons supprimés (uid)")

    # Dates
    date_replacements = {
        # ID marché invalide et SIRET de l'acheteur
        "0002-11-30": "",
        "September, 16 2021 00:00:00": "2021-09-16",  # 20007695800012 19830766200017 (plein !)
        "16 2021 00:00:00": "",
        "0222-04-29": "2022-04-29",  # 202201L0100
        "0021-12-05": "2022-12-05",  # 20222022/1400
        "0001-06-21": "",  # 0000000000000000 21850109600018
        "0019-10-18": "",  # 0000000000000000 34857909500012
        "5021-02-18": "2021-02-18",  # 20213051200 21590015000016
        "2921-11-19": "",  # 20220057201 20005226400013
        "0022-04-29": "2022-04-29",  # 2022AOO-GASL0100 25640454200035
    }

    # Using replace_many for efficient replacement of multiple date values
    df = df.with_columns(
        pl.col(["datePublicationDonnees", "dateNotification"])
        .str.replace_many(date_replacements)
        .cast(pl.Utf8)
    )

    # Nombres
    df = df.with_columns(
        pl.col(["dureeMois", "montant"]).replace("", None).cast(pl.Float64)
    )

    # Nature
    df = df.with_columns(
        pl.col("nature").str.replace_many(
            {"Marche": "Marché", "subsequent": "subséquent"}
        )
    )

    return df


def fix_data_types(df: pd.DataFrame):
    # ***  TYPES DE DONNÉES ***#

    numeric_dtypes = {
        "dureeMois": "Int64",  # contrairement à int64, Int64 autorise les valeurs nulles https://pandas.pydata.org/docs/user_guide/integer_na.html
        "montant": "float64",
    }

    for column in numeric_dtypes:
        df[column] = df[column].astype(numeric_dtypes[column])

    date_dtypes = ["datePublicationDonnees", "dateNotification"]

    for column in date_dtypes:
        df[column] = pd.to_datetime(df[column], format="mixed", dayfirst=True)

    return df
