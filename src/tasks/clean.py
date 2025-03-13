import pandas as pd
from numpy import nan as NaN


def clean_official_decp(df: pd.DataFrame):
    df.replace([NaN, None], "", inplace=True, regex=False)

    # Suppression des lignes en doublon
    # Exemple : 20005584600014157140791205100

    df = df.drop_duplicates(ignore_index=True)

    # Dates

    columns_date = ["datePublicationDonnees", "dateNotification"]

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

    for col in columns_date:
        df[col] = df[col].replace(date_replacements, regex=False)

    # Nombres

    df["dureeMois"] = df["dureeMois"].replace("", NaN)
    df["montant"] = df["montant"].replace("", NaN)

    # Identifiants de marchés

    id_replacements = {"[,\./]": "_"}

    df["id"] = df["id"].replace(id_replacements, regex=True)

    # Nature

    nature_replacements = {"Marche": "Marché", "subsequent": "subséquent"}

    df["nature"] = df["nature"].str.capitalize()
    df["nature"] = df["nature"].replace(nature_replacements, regex=True)

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
