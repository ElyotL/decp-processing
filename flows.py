from prefect import flow, get_run_logger
import datetime

@flow(retries=3, retry_delay_seconds=20)
def decp():
    # Timestamp
    date_now = datetime.date.today().isoformat()
    datetime_now = datetime.datetime.now().isoformat()