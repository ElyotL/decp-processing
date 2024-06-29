from prefect import flow
import pytest
from prefect.testing.utilities import prefect_test_harness
from flows import decp_processing
from dotenv import find_dotenv, load_dotenv
import os

from flows import decp_processing

from prefect.testing.utilities import prefect_test_harness


class TestFlow:
    @pytest.fixture(autouse=True, scope="class")
    def prefect_test_fixture(self):
        with prefect_test_harness():
            yield

    def test_flow(self):
        env_file = find_dotenv(".test.env")
        load_dotenv(env_file)
        decp_processing()
        assert not (os.getenv("DECP_ENRICHIES_VALIDES_URL").startswith("https"))
