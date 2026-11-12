__version__ = "0.1.0"

# The extension links libfsspec from the fsspec_rs package, which puts its lib/ on the DLL path.
import fsspec_rs

from .filesystem import KafkaFile, KafkaFileSystem, KafkaStream
from .fsspec_kafka import builtin_features, librdkafka_version, linked_to_libfsspec, list_topics

__all__ = ("KafkaFile", "KafkaFileSystem", "KafkaStream", "builtin_features", "librdkafka_version", "linked_to_libfsspec", "list_topics")
