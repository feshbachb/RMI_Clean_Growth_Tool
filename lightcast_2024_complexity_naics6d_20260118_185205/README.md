# Lightcast 2024 Economic Complexity Analysis

Generated: 2026-01-18 18:59:13.888086

Methodology: Daboin et al.

## File Formats
Each data frame is exported in multiple formats:
- **RDS**: R native format (all objects)
- **Parquet**: Efficient columnar format (non-geo)
- **CSV / CSV.gz**: Universal interchange (non-geo)
- **Geoparquet**: Efficient geo format (geo)
- **Geopackage (.gpkg)**: OGC standard geo format (geo)

## Data Frames
- `geography_industry_complexity_pairs`: All geo-industry pairs with LQ, density, strategic gain
- `geography_specific_data`: ECI, diversity, strategic index by geography
- `industry_specific_data`: ICI, ubiquity by industry
- `industry_space_edges/nodes`: Network data for industry co-location visualization
- `*_geometry`: Standardized MULTIPOLYGON boundaries for mapping

