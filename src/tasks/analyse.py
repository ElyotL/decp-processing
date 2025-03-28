import polars as pl
from datetime import datetime

from tasks.get import get_stats


def list_data_issues(df: pl.LazyFrame):
    df = df.collect()

    # Dates impossibles

    date_columns = [
        "dateNotification",
        "dateNotificationActeSousTraitance",
        "dateNotificationModificationModification",
        "dateNotificationModificationSousTraitanceModificationActeSousTraitance",
        "datePublicationDonnees",
        "datePublicationDonneesActeSousTraitance",
        "datePublicationDonneesModificationActeSousTraitance",
        "datePublicationDonneesModificationModification",
    ]

    for column in date_columns:
        print(
            "Dates impossibles dans la colonne ",
            column,
            ":",
            df.filter(
                (pl.col(column) < pl.date(2015, 1, 1))
                | (pl.col(column) > datetime.now())
            ).height,
        )


def generate_stats(df: pl.DataFrame):
    now = datetime.now()

    df_titulaires = pl.DataFrame(columns=["titulaire.id", "titulaire.typeIdentifiant"])

    for i in range(1, 4):
        df_temp = df[[f"titulaire_id_{i}", f"titulaire_typeIdentifiant_{i}"]]
        df_temp = df_temp.rename(
            columns={
                f"titulaire_id_{i}": "titulaire.id",
                f"titulaire_typeIdentifiant_{i}": "titulaire.typeIdentifiant",
            }
        )
        df_titulaires = pl.concat([df_titulaires, df_temp], ignore_index=True)

    df.to_pickle("data/decp_before_stats.pkl")

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
            # "2024_nb_notifications": df.loc[
            #     df["dateNotification"].str.startswith("2024")
            # ].index.size,
            "2024_nb_publications": df.loc[
                df["datePublicationDonnees"].dt.year == 2024
            ].index.size,
            "2023_nb_notifications": df.loc[
                df["dateNotification"].dt.year == 2023
            ].index.size,
            "2023_nb_publications": df.loc[
                df["datePublicationDonnees"].dt.year == 2023
            ].index.size,
            "nb_marches_format_arrete_2022": df.loc[df["ccag"] != ""].index.size,
        }
    ]

    # df_per_source = (
    #     df[["id", "acheteur.id", "source"]].drop_duplicates().groupby(by="source").count()
    # )

    # for idx in df_per_source.index:
    #     if idx == "":
    #         source = "source-manquante"
    #     else:
    #         source = idx

    #     stats[source] = df_per_source.loc[idx, "id"]

    df_stats_dgfr: pl.DataFrame = get_stats()
    df_stats_dgfr = pl.concat([df_stats_dgfr, pl.DataFrame(stats)], ignore_index=True)
    df_stats_dgfr.to_csv("dist/statistiques.csv")
