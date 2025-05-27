from prefect import flow
import os
from dotenv import dotenv_values

if __name__ == "__main__":
    env = dotenv_values()
    flow.from_source(
        source="https://github.com/ColinMaudry/decp-processing.git",
        entrypoint="src/flows.py:decp_processing",
    ).deploy(
        name="decp-processing",
        description="Tous les jours du lundi au vendredi à 6h00",
        work_pool_name="local",
        ignore_warnings=True,
        cron="0 6 * * 1-5",
        job_variables={
            # Les variables d'environnement sont sont donc fixées au moment du déploiement,
            # pas dans un .env au moment du run
            "env": env
        },
    )
