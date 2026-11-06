import os
from pathlib import Path

import pytest

from fsspec_kafka import list_topics

BOOTSTRAP = os.environ.get("KAFKA_TEST_BOOTSTRAP", "")
KERBEROS_BOOTSTRAP = os.environ.get("KAFKA_TEST_KERBEROS_BOOTSTRAP", "")
KEYTAB = Path(__file__).parents[3] / "ci" / "kerberos" / "keytabs" / "client.keytab"


@pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_TEST_BOOTSTRAP not set")
def test_plaintext_list_topics():
    assert isinstance(list_topics({"bootstrap.servers": BOOTSTRAP}), list)


@pytest.mark.skipif(not KERBEROS_BOOTSTRAP, reason="KAFKA_TEST_KERBEROS_BOOTSTRAP not set")
def test_kerberos_list_topics():
    config = {
        "bootstrap.servers": KERBEROS_BOOTSTRAP,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "GSSAPI",
        "sasl.kerberos.service.name": "kafka",
        "sasl.kerberos.principal": "client@FSSPEC.TEST",
        "sasl.kerberos.keytab": str(KEYTAB),
        # The broker port is published on 127.0.0.1 only; localhost may resolve to ::1 first.
        "broker.address.family": "v4",
    }
    assert isinstance(list_topics(config), list)
