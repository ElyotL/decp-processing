# DECP processing

Projet de traitement et de publication de meilleures données sur les marchés publics attribués en France. Ce projet prend sa source dans la complexité de la publication des données faite par le Ministère des Finances :

- code source de l'agrégation des données [fermé](https://github.com/139bercy/decp-rama-v2/blob/main/README.md)
- documentation incomplète et éparpillée
    - https://github.com/139bercy/decp-rama-v2/blob/main/README.md
    - https://www.data.gouv.fr/fr/datasets/5cd57bf68b4c4179299eb0e9
- schéma de données DECP 2 complexe à utiliser

Ce projet se veut collaboratif et à l'écoute des besoins des usagers potentiels : entreprises, acteurs publics, journalistes, chercheurs et chercheuses.

Pour me contacter vous pouvez ouvrir un "issue" sur Github ou me contacter par email [colin+decp@maudry.com](mailto:colin+decp@maudry.com).

## Ordre et description des notebooks

1. marchés : pour récupérer les données de base et les nettoyer
2. sirene_acheteurs : pour récupérer les noms des acheteurs depuis la base SIRENE
3. sirene_titulaires : pour récupérer les données des titulaires depuis la base SIRENE
4. publish : création du datapackage et publication sur data.gouv.fr
