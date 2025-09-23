use pyo3::prelude::*;
use geo::geometry::Point, HaversineDistance;

#[pyfunction]
fn point_to_point_distance(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pymodule]
fn nvdb_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(point_to_point_distance, m)?)?;
    Ok(())
}
