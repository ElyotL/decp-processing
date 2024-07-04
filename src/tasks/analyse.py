import pandas as pd
from tasks.get import get_stats
from datetime import datetime


def generate_stats(df: pd.DataFrame):
    now = datetime.now()

    df_titulaires = pd.DataFrame(columns=["titulaire.id", "titulaire.typeIdentifiant"])

    for i in range(1, 4):
        df_temp = df[[f"titulaire_id_{i}", f"titulaire_typeIdentifiant_{i}"]]
        df_temp = df_temp.rename(
            columns={
                f"titulaire_id_{i}": "titulaire.id",
                f"titulaire_typeIdentifiant_{i}": "titulaire.typeIdentifiant",
            }
        )
        df_titulaires = pd.concat([df_titulaires, df_temp], ignore_index=True)

    stats = [
        {
            "datetime": now.isoformat()[:-7],  # jusqu'aux secondes
            "année": str(now.year),
            "mois": str(now.month).zfill(2),
            "dataset": "decp-augmente-minef",
            "nb_lignes": df.index.size,
            "nb_colonnes": len(df.columns),
            "nb_marches": df[["id", "acheteur.id"]].drop_duplicates().index.size,
            "nb_acheteurs_uniques": df[["acheteur.id"]].drop_duplicates().index.size
            - 1,  # -1 pour ne pas compter la valeur "acheteur vide"
            "nb_titulaires_uniques": df_titulaires.drop_duplicates().index.size
            - 1,  # -1 pour ne pas compter la valeur "titulaire vide"
            "2024_nb_notifications": df.loc[
                df["dateNotification"].str.startswith("2024")
            ].index.size,
            "2024_nb_publications": df.loc[
                df["datePublicationDonnees"].str.startswith("2024")
            ].index.size,
            "2023_nb_notifications": df.loc[
                df["dateNotification"].str.startswith("2023")
            ].index.size,
            "2023_nb_publications": df.loc[
                df["datePublicationDonnees"].str.startswith("2023")
            ].index.size,
            "nb_marches_format_arrete_2022": df.loc[df["ccag"] != ""].index.size,
        }
    ]

    df_per_source = (
        df[["id", "acheteur.id"]].drop_duplicates().groupby(by="source").count()
    )

    for idx in df_per_source.index:
        if idx == "":
            source = "source-manquante"
        else:
            source = idx

        stats[source] = df_per_source.loc[idx, "id"]

    df_stats_dgfr: pd.DataFrame = get_stats()
    df_stats_dgfr = pd.concat([df_stats_dgfr, pd.DataFrame(stats)], ignore_index=True)
    df_stats_dgfr.to_csv("dist/statistiques.csv")
