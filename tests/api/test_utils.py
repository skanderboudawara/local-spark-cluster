import pytest
from pyspark.sql import SparkSession
from src.compute._utils import filter_kwargs, spark_session

class TestFilterKwargs:
    def test_empty_dict(self):
        assert filter_kwargs({}, int) == {}

    def test_mixed_types(self):
        kwargs = {'a': 1, 'b': 'string', 'c': 3.0, 'd': 4}
        expected = {'a': 1, 'd': 4}
        assert filter_kwargs(kwargs, int) == expected

    def test_all_match(self):
        kwargs = {'a': 1, 'b': 2, 'c': 3}
        expected = {'a': 1, 'b': 2, 'c': 3}
        assert filter_kwargs(kwargs, int) == expected

    def test_none_match(self):
        kwargs = {'a': 'string', 'b': 3.0, 'c': []}
        assert filter_kwargs(kwargs, int) == {}

    def test_different_type(self):
        kwargs = {'a': 1, 'b': 'string', 'c': 3.0, 'd': [1, 2, 3]}
        expected = {'d': [1, 2, 3]}
        assert filter_kwargs(kwargs, list) == expected




class TestSparkSession:
    def test_spark_session_default(self):
        session = spark_session("test_app")
        assert isinstance(session, SparkSession)
        assert session.sparkContext.appName == "test_app"
        assert session.conf.get("spark.master") == "spark://spark-master:7077"
        session.stop()

    def test_spark_session_with_conf(self):
        conf = {
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2"
        }
        session = spark_session("test_app_with_conf", conf)
        assert isinstance(session, SparkSession)
        assert session.sparkContext.appName == "test_app_with_conf"
        assert session.conf.get("spark.executor.memory") == "2g"
        assert session.conf.get("spark.executor.cores") == "2"
        session.stop()