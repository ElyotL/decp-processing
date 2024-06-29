import pandas as pd
from httpx import get
import os
from prefect import task, get_run_logger
from pathlib import Path


@task
def get_official_decp(date_now: str):
    if os.getenv("DECP_ENRICHIES_VALIDES_URL").startswith("https"):
        # Prod file
        decp_augmente_valides_file: Path = Path(
            f"data/decp_augmente_valides_{date_now}.csv"
        )
    else:
        # Test file, pas de téléchargement
        decp_augmente_valides_file: Path = Path(os.getenv("DECP_ENRICHIES_VALIDES_URL"))

    if not (os.path.exists(decp_augmente_valides_file)):
        request = get(os.getenv("DECP_ENRICHIES_VALIDES_URL"))
        with open(decp_augmente_valides_file, "wb") as file:
            file.write(request.content)
    else:
        print(f"DECP d'aujourd'hui déjà téléchargées ({date_now})")

    df: pd.DataFrame = pd.read_csv(
        decp_augmente_valides_file,
        sep=";",
        dtype="object",
        index_col=None,
    )

    return df
