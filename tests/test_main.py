from prefect.testing.utilities import prefect_test_harness
from flows import decp_processing

print("avant test")


class TestFlow:
    def test_flow(self):
        with prefect_test_harness():
            result = decp_processing()
