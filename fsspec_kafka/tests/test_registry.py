"""The kafka protocol is registered in libfsspec, so any consumer of the shared library can open it."""

from ctypes import CDLL, POINTER, Structure, byref, c_char_p, c_size_t, c_void_p, string_at
from pathlib import Path
from sys import platform

import fsspec_rs
import pytest
from fsspec_rs.fsspec_rs import protocols

import fsspec_kafka


class String(Structure):
    _fields_ = [("data", c_void_p), ("len", c_size_t)]


@pytest.fixture(scope="module")
def lib():
    if not fsspec_kafka.linked_to_libfsspec():
        pytest.skip("this build keeps its own registry")
    name = {"win32": "fsspec.dll", "darwin": "libfsspec.dylib"}.get(platform, "libfsspec.so")
    lib = CDLL(str(Path(fsspec_rs.get_library_dirs()[0]) / name))
    lib.fsspec_last_error_message.restype = c_char_p
    lib.fsspec_options_new.restype = c_void_p
    lib.fsspec_options_set.argtypes = [c_void_p, c_char_p, c_size_t, c_char_p, c_size_t]
    lib.fsspec_options_free.argtypes = [c_void_p]
    lib.fsspec_url_to_fs.argtypes = [c_char_p, c_size_t, c_void_p, POINTER(c_void_p), POINTER(String)]
    lib.fsspec_fs_release.argtypes = [c_void_p]
    lib.fsspec_string_free.argtypes = [String]
    return lib


def test_kafka_is_registered_in_the_shared_registry():
    assert fsspec_kafka.linked_to_libfsspec() == fsspec_rs.fsspec_rs.linked_to_libfsspec()
    assert "kafka" in protocols()


def test_libfsspec_opens_kafka_urls(lib):
    # Building the filesystem does not connect to the cluster, so a dead address is fine.
    options = lib.fsspec_options_new()
    assert lib.fsspec_options_set(options, b"bootstrap.servers", 17, b"localhost:1", 11) == 0
    fs = c_void_p()
    path = String()
    url = b"kafka://topic"
    assert lib.fsspec_url_to_fs(url, len(url), options, byref(fs), byref(path)) == 0, lib.fsspec_last_error_message()
    lib.fsspec_options_free(options)
    assert string_at(path.data, path.len) == b"topic"
    lib.fsspec_string_free(path)
    lib.fsspec_fs_release(fs)
