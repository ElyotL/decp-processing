from prefect.testing.utilities import prefect_test_harness
from flows import decp_processing

print("avant test")


class TestFlow:
    def test_flow(self):
        print("avant with harness")
        with prefect_test_harness():
            print("Début test")
            decp_processing()
