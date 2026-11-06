import pytest

from fsspec_kafka import builtin_features, librdkafka_version


def test_librdkafka_version():
    assert librdkafka_version().startswith("2.")


@pytest.mark.parametrize("feature", ["ssl", "sasl_gssapi", "sasl_scram", "sasl_oauthbearer", "zstd"])
def test_builtin_feature(feature):
    assert feature in builtin_features()
