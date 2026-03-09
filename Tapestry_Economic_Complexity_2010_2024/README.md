# Tapestry_Economic_Complexity_2010_2024

## Overview

Economic complexity analysis of U.S. employment data from the Tapestry QCEW database,
computed for years 2010 through 2024 using the Daboin et al. (2019) eigenvector
methodology. All metrics (ECI, ICI, proximity, density, strategic gain) are computed
at five geographic levels: county, state, CBSA, CSA, and commuting zone.

Generated: 2026-03-09 17:08:46

## Data Sources

| Source | Description | URL |
|--------|-------------|-----|
| Tapestry QCEW | Employment data (Rule 3) | https://tapestry.nkn.uidaho.edu |
| BLS | NAICS hierarchy crosswalk | https://www.bls.gov/cew/classifications/industry/ |
| Census LODES | Block-level employment for CT/AK harmonization | https://lehd.ces.census.gov |
| TIGRIS 2024 | County, CBSA, CSA boundaries | via R `tigris` package |
| USDA ERS | 2020 Commuting Zone definitions | https://www.ers.usda.gov |

## Methodology

All formulas follow Daboin et al. (2019), *Economic Complexity and Technological
Relatedness: Findings for American Cities*. No minimum ubiquity filtering is applied;
the M matrix is M[c,i] = 1{RCA >= 1} directly per equations 4-5.

### Revealed Comparative Advantage (RCA) — Eq. 4
```
LQ[c,i] = (emp[c,i] / emp[c]) / (emp[nation,i] / emp[nation])
M[c,i]  = 1 if LQ[c,i] >= 1, else 0    (Eq. 5)
```

### ECI/ICI — Eqs. 12-14
```
M_tilde_C[c,c'] = sum_i M[c,i]*M[c',i] / (diversity[c] * ubiquity[i])
ECI = 2nd eigenvector of M_tilde_C
ICI = 2nd eigenvector of M_tilde_I (dual)
```
Sign correction: ICI negatively correlated with ubiquity; ECI positively
correlated with average ICI of specialized industries.

### Proximity — Eq. 16
```
phi[i,j] = U[i,j] / max(ubiquity[i], ubiquity[j])
```

### Density (Feasibility) — Eq. 19
```
density[c,i] = sum_j M[c,j]*phi[j,i] / sum_j phi[j,i]
```

### Centrality — Eq. 17
```
centrality[i] = sum_j phi[i,j] / sum_all phi
```

### Strategic Index — Eq. 21
```
SI[c] = sum_i density[c,i] * (1-M[c,i]) * ICI[i]
```

### Strategic Gain — Eq. 22
```
SG[c,i] = sum_j (phi_norm[i,j]*(1-M[c,j])*ICI[j]) - density[c,i]*ICI[i]
```

### State-Level Aggregation (v2.4)

State ECI = employment-weighted mean of county ECI values.
State density/strategic gain = employment-weighted mean of county-level matrices.
State M = binary RCA from state-level LQ.

## Geographic Harmonization

### Connecticut (2010-2023)
Eight old counties -> nine planning regions via LODES employment-weighted allocation.

### Alaska (2010-2019)
Valdez-Cordova (02261) -> Chugach (02063) + Copper River (02066) via LODES.

### Other GEOID Corrections
- Wade Hampton (02270) -> Kusilvak (02158)
- Shannon County (46113) -> Oglala Lakota (46102)
- Bedford City (51515) -> Bedford County (51019)

## NAICS Code Aggregation

- BLS specialty trade contractor codes: 238XX1/238XX2 collapsed to 238XX0
- Three-digit rollups: 812XXX->812000, 111XXX->111000, 112XXX->112000, 51319X->513190

## Directory Structure

All data files use gzipped CSV (.csv.gz) for efficient storage and transfer.
County-level data is consolidated into one file per state; other geographic
levels use a single file per level.

```
Tapestry_Economic_Complexity_2010_2024/
  crosswalks/
    county_crosswalk.csv.gz
    naics_hierarchy_v2022.csv.gz  (etc.)
  industry_space/
    industry_space_edges.csv.gz
    industry_space_nodes.csv.gz
  peer_geographies/
    peer_geographies.csv.gz
  {year}/                              (e.g., 2024/)
    diagnostics.csv.gz
    industry_geography_data/
      by_industry/
        all_industries.csv.gz          (all NAICS codes in one file)
      by_geography/
        county/{State_Name}_county_level.csv.gz   (one per state)
        state/all_state.csv.gz
        core_based_statistical_area/all_cbsa.csv.gz
        combined_statistical_area/all_csa.csv.gz
        commuting_zone/all_cz.csv.gz
    industry_specific_data/
      all_industries.csv.gz            (ICI, ubiquity, percentiles)
    geography_specific_data/
      by_geography/
        county/{State_Name}_county_level.csv.gz
        state/all_state.csv.gz
        (same pattern as above)
  README.md
  Tapestry_Economic_Complexity_2010_2024.R
```

## Geographic Levels

| Level | Description | Complexity Method |
|-------|-------------|-------------------|
| County | ~3,100 US counties (2024 TIGRIS) | Native eigenvector |
| State | 50 states + DC | v2.4 county aggregation (density/SG emp-weighted) |
| CBSA | Core Based Statistical Areas (2024) | Native eigenvector |
| CSA | Combined Statistical Areas (2024) | Native eigenvector |
| CZ | 2020 Commuting Zones (USDA ERS) | Native eigenvector |

## References

- Daboin, C., Johnson, S., Kotsadam, A., Neffke, F., & O'Clery, N. (2019).
  *Economic Complexity and Technological Relatedness: Findings for American Cities.*
- Hausmann, R. & Hidalgo, C. (2009). *The building blocks of economic complexity.*
  PNAS, 106(26), 10570-10575.
- Escobari, M., Seyal, I., Morales-Arilla, J., & Shearer, C.
  *Growing Cities that Work for All.* Brookings Institution.
