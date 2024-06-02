# DECP processing

Projet de traitement et de publication de meilleures données sur les marchés publics attribués en France. Ce projet prend sa source dans la complexité de la publication des données faite par le Ministère des Finances :

- code source de l'agrégation des données [fermé](https://github.com/139bercy/decp-rama-v2/blob/main/README.md)
- documentation incomplète et éparpillée
    - https://github.com/139bercy/decp-rama-v2/blob/main/README.md
    - https://www.data.gouv.fr/fr/datasets/5cd57bf68b4c4179299eb0e9
    - https://data.economie.gouv.fr/pages/donnees-essentielles-de-la-commande-publique/
- schéma de données DECP 2 [complexe à utiliser](https://github.com/ColinMaudry/decp-processing/issues/4)

Ce projet se veut collaboratif et à l'écoute des besoins des usagers potentiels : entreprises, acteurs publics, journalistes, chercheurs et chercheuses, citoyens et citoyennes.

Pour me contacter vous pouvez ouvrir un "issue" sur Github ou me contacter par email [colin+decp@maudry.com](mailto:colin+decp@maudry.com).

## Données

Les données produites sont les mêmes données que celles publiées par le Ministère des Finances sur data.economie.gouv.fr. J'ai choisi de prendre ces données comme source et non les DECP au format réglementaire JSON car les premières ont été nettoyées et améliorées ([code](https://github.com/139bercy/decp-augmente)) par le Ministère, ce qui me fait moins de travail.

Elles sont mises à dispositions aux formats CSV, Parquet et SQLite.

Vous pouvez...

- les télécharger sur [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/donnees-essentielles-de-la-commande-publique-consolidees-format-tabulaire/) (vous trouverez aussi plus d'informations sur ces données)
- les visualiséer, les filtrer et télécharger sur [decp.info](https://decp.info)

## Utilisation

Je vous recommande d'utiliser un environnement virtuel Python pour isoler l'installation des dépendances :

```bash
virtualenv .venv
```

Activez l'environnement virtuel :

```bash
source .venv/bin/activate
```

Installez les dépendances :

```bash
pip install .
```

Lancez Jupyter notebook (je n'ai pas trop testé, j'utilise l'intégration dans VS Code) :

```bash
jupyter notebook
```

Ordre d'exécution et description des notebooks :

1. marchés : pour récupérer les données de base et les nettoyer
2. sirene_acheteurs : pour récupérer les noms des acheteurs depuis la base SIRENE
3. sirene_titulaires : pour récupérer les données des titulaires depuis la base SIRENE
4. publish : création du datapackage, de la base de données SQLite, et publication sur data.gouv.fr
