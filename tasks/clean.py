import pandas as pd

def clean_official_decp(df: pd.DataFrame):
    # Dates

    columns_date = ["datePublicationDonnees", "dateNotification"]

    date_replacements = {
        # ID marché invalide et SIRET de l'acheteur
        "0002-11-30": "",
        "September, 16 2021 00:00:00": "2021-09-16", #20007695800012 19830766200017 (plein !)
        "16 2021 00:00:00": "",
        "0222-04-29": "2022-04-29", # 202201L0100
        "0021-12-05": "2022-12-05", # 20222022/1400
        "0001-06-21": "", #0000000000000000 21850109600018
        "0019-10-18": "", #0000000000000000 34857909500012
        "5021-02-18": "2021-02-18", #20213051200 21590015000016   
        "2921-11-19": "", # 20220057201 20005226400013
        "0022-04-29": "2022-04-29", # 2022AOO-GASL0100 25640454200035   
    }

    for col in columns_date:
        df[col] = df[col].replace(date_replacements, regex=False)


    # Nombres

    df['dureeMois'] = df['dureeMois'].replace('', NaN)
    df['montant'] = df['montant'].replace('', NaN)

    # Identifiants de marchés

    id_replacements = {
        "[,\./]": "_"
    }

    df["id"] = df["id"].replace(id_replacements, regex=True)

    return df