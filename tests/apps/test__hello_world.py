from utils.testing import create_all_resoures, drop_test_tables
from utils.utils import start_session
from apps.hello_world import run
import pytest
import os


@pytest.fixture()
def spark():
    os.environ.setdefault("RUN_MODE", "TEST")
    spark = start_session("test_hello_app")
    create_all_resoures(spark)
    yield spark
    drop_test_tables(spark)
    spark.stop()


def test_application(spark):
    assert run(spark) is None
