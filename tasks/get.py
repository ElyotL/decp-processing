import pandas as pd
import os
from prefect import task, get_run_logger


@task
def get_official_decp():
    df: pd.DataFrame = pd.read_csv(os.getenv('DECP_ENRICHIES_VALIDES_URL'), sep=";", dtype='object', index_col=None)
    return df


