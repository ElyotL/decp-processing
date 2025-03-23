from prefect.testing.utilities import prefect_test_harness
from flows import make_datalab_data
import polars as pl


class TestFlow:
    def test_datalab_output(self):
        with prefect_test_harness():
            df: pl.DataFrame = make_datalab_data()

            assert df.height > 1
