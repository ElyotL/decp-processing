from prefect import flow, get_run_logger
import datetime
from dotenv import load_dotenv
import subprocess

from tasks.get import *
from tasks.clean import *
from tasks.transform import *
from tasks.output import *
from tasks.analyse import *

# from tasks.test import *
# from tasks.enrich import *


@flow(log_prints=True)
def decp_processing():
    # TRAITEMENT MARCHÉS

    load_dotenv()
    logger = get_run_logger()

    # Timestamp
    date_now = datetime.now().isoformat()[0:10]  # YYYY-MM-DD

    # git pull
    print("Récupération du code (pull)...")
    command = "git pull origin prefect"
    subprocess.run(command.split(" "))

    print("Création du dossier dist/")
    if os.path.exists("dist"):
        print("dist exists")
    else:
        os.mkdir("dist")

    print("Récupération des données source...")
    df: pd.DataFrame = get_and_merge_decp_csv(date_now)
    logger.info(f"DECP officielles: nombre de lignes: {df.index.size}")
    save_to_sqlite(df, "datalab", "data.economie.2019.2022")

    print("Ajout du champ uid")
    df["uid"] = df["acheteur.id"] + df["id"]

    print("Nettoyage des données source...")
    df = clean_official_decp(df)

    print("Typage des colonnes...")
    df = fix_data_types(df)

    print("Analyse des données source...")
    generate_stats(df)

    print("Concaténation et explosion des titulaires, un par ligne...")
    df = explode_titulaires(df)

    # print("Ajout des colonnes manquantes...")
    # df = setup_tableschema_columns(df)

    # Quand https://github.com/ColinMaudry/decp-processing/issues/17 sera résolu
    # à supprimer
    print("Enregistrement des DECP aux formats CSV et Parquet...")
    save_to_files(df, "dist/decp")

    # DONNÉES SIRENE ACHETEURS

    # Enrichissement des données pas prioritaire
    # cf https://github.com/ColinMaudry/decp-processing/issues/17

    # print("Extraction des SIRET des acheteurs...")
    # df_sirets_acheteurs = extract_unique_acheteurs_siret(df)

    # print("Ajout des données établissements (acheteurs)...")
    # df_sirets_acheteurs = add_etablissement_data_to_acheteurs(df_sirets_acheteurs)

    # print("Ajout des données unités légales (acheteurs)...")
    # df_sirets_acheteurs = add_unite_legale_data_to_acheteurs(df_sirets_acheteurs)

    # print("Construction du champ acheteur.nom à partir des données SIRENE...")
    # df_sirets_acheteurs = make_acheteur_nom(df_sirets_acheteurs)

    # print("Jointure des données acheteurs enrichies avec les DECP...")
    # df = merge_sirets_acheteurs(df, df_sirets_acheteurs)
    # del df_sirets_acheteurs

    # print("Enregistrement des DECP aux formats CSV et Parquet...")
    # save_to_files(df, "dist/decp")

    # print("Suppression de colonnes et déduplication pour les DECP Sans Titulaires...")
    # df_decp_sans_titulaires = make_decp_sans_titulaires(df)
    # save_to_files(df_decp_sans_titulaires, "dist/decp-sans-titulaires")
    # del df_decp_sans_titulaires

    # DONNÉES SIRENE TITULAIRES

    # Enrichissement des données pas prioritaire
    # cf https://github.com/ColinMaudry/decp-processing/issues/17

    # print("Extraction des SIRET des titulaires...")
    # df_sirets_titulaires = extract_unique_titulaires_siret(df)

    # print("Ajout des données établissements (titulaires)...")
    # df_sirets_titulaires = add_etablissement_data_to_titulaires(df_sirets_titulaires)

    # print("Ajout des données unités légales (titulaires)...")
    # df_sirets_titulaires = add_unite_legale_data_to_titulaires(df_sirets_titulaires)

    # print("Amélioration des données unités légales des titulaires...")
    # df_sirets_titulaires = improve_titulaire_unite_legale_data(df_sirets_titulaires)

    # print("Renommage de certaines colonnes unités légales (titulaires)...")
    # df_sirets_titulaires = rename_titulaire_sirene_columns(df_sirets_titulaires)

    # print("Jointure pour créer les données DECP Titulaires...")
    # df_decp_titulaires = merge_sirets_titulaires(df, df_sirets_titulaires)
    # del df_sirets_titulaires

    # print("Enregistrement des DECP Titulaires aux formats CSV et Parquet...")
    # save_to_files(df_decp_titulaires, "dist/decp-titulaires")
    # del df_decp_titulaires

    # CREATION D'UN DATA PACKAGE (FRICTIONLESS DATA) ET DES FICHIERS DATASETTE

    # if not (os.curdir.endswith("dist")):
    #     os.chdir("./dist")
    #     print(os.curdir)
    #
    # print("Validation des données DECP avec le TableSchema...")
    # validate_decp_against_tableschema()
    #
    # print("Création du data package (JSON)....")
    # make_data_package()
    #
    # print("Création de la DB SQLite et des métadonnées datasette...")
    # make_sqllite_and_datasette_metadata()

    # PUBLICATION DES FICHIERS SUR DATA.GOUV.FR

    # quand ce sera stable !


if __name__ == "__main__":
    decp_processing()

    # On verra les deployments quand la base marchera
    #
    # decp_processing.serve(
    #     name="decp-processing-cron",
    #     cron="0 6 * * 1-5",
    #     description="Téléchargement, traitement, et publication des DECP.",
    # )
    # decp_processing.serve(
    #     name="decp-processing-once",
    #     description="Téléchargement, traitement, et publication des DECP.",
    # )
