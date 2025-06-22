import polars as pl

TITULAIRE_SCHEMA_2022 = pl.Struct(
    {
        "titulaire": pl.Struct(
            {
                "typeIdentifiant": pl.String,
                "id": pl.String,
            }
        )
    }
)

MODIFICATION_SCHEMA_2022 = pl.Struct(
    {
        "modification": pl.Struct(
            {
                "id": pl.Int32,
                # can switch down to UInt8 when https://github.com/pola-rs/polars/pull/16105 is merged
                "dateNotificationModification": pl.String,
                "datePublicationDonneesModification": pl.String,
                "montant": pl.String,
                "dureeMois": pl.String,
                "titulaires.typeIdentifiant": pl.String,
                "titulaires.id": pl.String,
            }
        )
    }
)

MARCHE_SCHEMA_2022 = {
    "procedure": pl.String,
    "nature": pl.String,
    "codeCPV": pl.String,
    "dureeMois": pl.String,
    "datePublicationDonnees": pl.String,
    "titulaires": pl.List(TITULAIRE_SCHEMA_2022),
    "modifications": pl.List(MODIFICATION_SCHEMA_2022),
    "id": pl.String,
    "formePrix": pl.String,
    "dateNotification": pl.String,
    "objet": pl.String,
    "montant": pl.String,
    "acheteur.id": pl.String,
    "source": pl.String,
    "lieuExecution.code": pl.String,
    "lieuExecution.typeCode": pl.String,
    "uid": pl.String,
    "considerationsSociales": pl.List(pl.String),
    "considerationsEnvironnementales": pl.List(pl.String),
    "marcheInnovant": pl.String,
    "attributionAvance": pl.String,
    "sousTraitanceDeclaree": pl.String,
    "ccag": pl.String,
    "offresRecues": pl.String,
    "typeGroupementOperateurs": pl.String,
    "idAccordCadre": pl.String,
    "tauxAvance": pl.String,
    "origineUE": pl.String,
    "origineFrance": pl.String,
}
