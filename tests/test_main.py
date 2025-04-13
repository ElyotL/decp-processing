from prefect.testing.utilities import prefect_test_harness
from flows import make_datalab_data
import polars as pl
import pytest
import logging
import os


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture(tmp_path_factory):
    """Fires up a temporary local SQLite database for testing Prefect flows"""
    # Thanks Tom Matthews https://linen.prefect.io/t/23466101/ulva73b9p-when-i-test-my-flow-in-pytest-with-prefect-test-ha
    os.environ["PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS"] = "90"

    with prefect_test_harness():
        # Clean up all loggers and handlers
        loggers_to_cleanup = [
            logging.getLogger(),  # Root logger
        ]

        for lgr in loggers_to_cleanup:
            for handler in lgr.handlers[:]:
                if "prefect" in str(handler).lower():
                    print(f"Flushing Prefect handler: {handler}")
                    handler.flush()
                    handler.close()
                lgr.removeHandler(handler)


class TestFlow:
    def test_datalab_output(self):
        # with prefect_test_harness(server_startup_timeout=10):
        make_datalab_data()
