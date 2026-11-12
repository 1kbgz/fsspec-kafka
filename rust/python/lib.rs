use std::collections::HashMap;
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

mod fs;
mod registry;

/// Version of the linked librdkafka.
#[pyfunction]
fn librdkafka_version() -> String {
    ::fsspec_kafka::librdkafka_version()
}

/// Features compiled into the linked librdkafka (`builtin.features`).
#[pyfunction]
fn builtin_features() -> PyResult<Vec<String>> {
    ::fsspec_kafka::builtin_features().map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// Topic names in the cluster. `config` holds librdkafka properties.
#[pyfunction]
#[pyo3(signature = (config, timeout=10.0))]
fn list_topics(py: Python, config: HashMap<String, String>, timeout: f64) -> PyResult<Vec<String>> {
    py.detach(|| ::fsspec_kafka::list_topics(&config, Duration::from_secs_f64(timeout)))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

#[pymodule]
fn fsspec_kafka(m: &Bound<PyModule>) -> PyResult<()> {
    registry::register()?;
    m.add_function(wrap_pyfunction!(registry::linked_to_libfsspec, m)?)?;
    m.add_function(wrap_pyfunction!(librdkafka_version, m)?)?;
    m.add_function(wrap_pyfunction!(builtin_features, m)?)?;
    m.add_function(wrap_pyfunction!(list_topics, m)?)?;
    m.add_class::<fs::RustKafkaFs>()?;
    m.add_class::<fs::RustKafkaFile>()?;
    m.add_class::<fs::RustKafkaStream>()?;
    Ok(())
}
