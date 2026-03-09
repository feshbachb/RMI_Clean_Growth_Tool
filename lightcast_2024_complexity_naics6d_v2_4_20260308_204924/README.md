# Lightcast 2024 Economic Complexity Analysis (v2.4)

Generated: 2026-03-08 21:00:03.596582

Methodology: Daboin et al.

## v2.4 Changes
- State-level ECI uses county-aggregated (employment-weighted) values
- State-level ICI uses county-level ICI as proxy
- State-level feasibility/density aggregated from county-level density matrices
- Geography-specific and industry-specific data frames broken out by level
- Fixed CBSA cross-validation: uses in-state employment, not total CBSA footprint
- Enhanced debugging throughout

## File Formats
- **RDS**: R native
- **Parquet**: Columnar
- **CSV / CSV.gz**: Comma-separated
- **TSV / TSV.gz**: Tab-separated
- **Geoparquet / Geopackage**: Geo formats

