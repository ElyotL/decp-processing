from prefect import flow
import pytest
from prefect.testing.utilities import prefect_test_harness
from flows import decp_processing
import os

from flows import decp_processing

from prefect.testing.utilities import prefect_test_harness


class TestFlow:
    @pytest.fixture(autouse=True, scope="class")
    def prefect_test_fixture(self):
        with prefect_test_harness():
            yield

    def test_flow(self):
        decp_csv_file = os.getenv("DECP_ENRICHIES_VALIDES_URL")
        print(decp_csv_file)
        assert not (decp_csv_file.startswith("https"))
        decp_processing()
