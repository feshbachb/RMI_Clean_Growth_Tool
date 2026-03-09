# RMI Clean Growth Tool — Comprehensive Audit & Map Improvement Plan

**Date:** March 9, 2026
**Scope:** Full repo audit (excluding Tapestry and "large files" release assets)

---

## Part 1: Repository Audit

### 1.1 Architecture Overview

The tool is a **static client-side dashboard** served via GitHub Pages from the `/docs` directory. There is no backend server — all data processing happens in the browser.

| Layer | Technology |
|-------|-----------|
| Frontend | Vanilla JS (1,970 lines in one file), HTML5, CSS3 |
| Charting | Plotly.js 2.27.0 (scatter, histogram, choropleth) |
| Mapping | D3.js 7.9.0 + TopoJSON (county-level choropleths) |
| CSV parsing | PapaParse 5.4.1 |
| Compression | Pako 2.1.0 (client-side gzip decompression) |
| Data pipeline | Python 3 + Pandas (`split_release_data.py`) |
| Hosting | GitHub Pages (Jekyll workflow) |

### 1.2 File Inventory (What Ships in `/docs`)

| Path | Size | Purpose |
|------|------|---------|
| `index.html` | 1,970 lines | Entire dashboard (HTML + CSS + JS in one file) |
| `us-counties-2023.json` | 3.4 MB | GeoJSON county boundaries |
| `rmi_logo_white.svg` / `rmi_logo_dark.svg` | ~small | Branding |
| `meta/` (18 CSV files) | ~15 MB | Crosswalks, geography/industry metadata, network data |
| `by_geography/{level}/{geoid}.csv.gz` | ~100 MB | Per-region industry data (3,143 counties + states/CBSAs/CSAs/CZs) |
| `by_industry/{level}/{industry_code}.csv.gz` | ~85 MB | Per-industry geography data |

**Total deployed size: ~203 MB** (mostly compressed CSV data)

### 1.3 Data Pipeline (`split_release_data.py`)

The Python script converts raw Lightcast data into dashboard-optimized format:
- Auto-detects latest Lightcast folder
- Splits large pair files by industry code and geography ID
- Compresses with gzip (50-60% reduction)
- Outputs ~4,076 files in ~200 MB
- Rounds floats to 6 decimals

### 1.4 Three Dashboard Tabs

1. **Regional View** — Select a geography, see its economic complexity profile (ECI, diversity, strategic index), tables of strengths/opportunities/strategic targets, and a feasibility-vs-complexity scatter plot.
2. **National View** — Compare a selected metric across all regions at a given geographic level with a choropleth map, summary stats, sortable table, and histogram.
3. **Industry Space** — D3-force network graph of 948 industries with ~4M+ proximity edges, filterable by threshold percentage, with optional geography highlighting.

### 1.5 Data Coverage

| Level | Count | Data Files |
|-------|-------|-----------|
| County | 3,143 | 3,143 compressed CSVs |
| State | 51 | 51 |
| CBSA | 927 | 927 |
| CSA | 183 | 183 |
| CZ | 590 | 590 |
| Industries | 948 | 948 per level |

Each geography-industry pair has 11 columns: `geoid, industry_code, industry_present, location_quotient, industry_comparative_advantage, industry_feasibility, industry_feasibility_percentile_score, strategic_gain, strategic_gain_percentile_score, strategic_gain_possible, industry_employment_share`.

### 1.6 Release Assets (v20260308_204924)

Ignoring "large files" and Tapestry, the release contains:

| Asset | Size | Format |
|-------|------|--------|
| CBSA geometry | 48.9 / 51.3 / 36.1 MB | geoparquet / gpkg / rds |
| County geometry | 116 / 123 / 83.9 MB | geoparquet / gpkg / rds |
| County-industry complexity pairs | 241 / 93.6 / 78.4 / 80.7 MB | csv / csv.gz / parquet / rds |

**Key finding: The dashboard does NOT currently use the release's geoparquet/gpkg boundary files.** It uses a separate `us-counties-2023.json` GeoJSON file in `/docs`. The release geometry assets are for external GIS/R/Python analysis, not the web dashboard.

### 1.7 Root-Level Files (Outside `/docs`)

| File | Purpose | Used by dashboard? |
|------|---------|-------------------|
| `split_release_data.py` | Data pipeline script | No (build-time only) |
| `README.md` | Brief project description | No |
| `.github/workflows/jekyll-gh-pages.yml` | CI/CD deployment | Yes (infra) |
| `RMI_logo_PrimaryUse.eps` / `..._White_Horizontal.eps` | Print logos | No |
| `d3-maps-choropleth.*.webarchive` | Archived D3 map examples | **No — reference only** |
| `dw-2.0.min.*.webarchive` | Archived DataWrapper lib | **No** |
| `jquery-3.2.1.min.webarchive` | Archived jQuery | **No** |
| `styles_dw.webarchive` | Archived DW styles | **No** |
| `us-counties-2023.*.webarchive` | Archived county data | **No** |

**Finding: The `.webarchive` files at root are developer reference materials, not used by the live dashboard. They could be moved to a `references/` folder or removed from the repo to reduce clutter.**

### 1.8 Known Fixed Issues (PRs #5 and #6)

1. **FIPS leading-zero bug** — PapaParse's `dynamicTyping` stripped leading zeros from FIPS codes (01→1), breaking data loading for 9 states. Fixed with `padGeoid()`.
2. **Choropleth color compression** — Extreme outliers (LQ up to 260) compressed the color spectrum. Fixed with percentile-based clipping (2nd–98th).
3. **County rendering** — Counties rendered as solid blocks instead of individual regions. Fixed with correct Plotly/D3 parameters.
4. **Industry Space layout** — Improved with seeded PRNG, 300 iterations, increased repulsion.

---

## Part 2: Map Implementation — Current State

### 2.1 Two Separate Map Renderers

The dashboard uses **two completely different map rendering approaches** depending on the geographic level:

| Renderer | Used for | Library | Approach |
|----------|----------|---------|----------|
| `renderStateMap()` | State-level only | **Plotly.js** | `type: 'choropleth'` with `locationmode: 'USA-states'`, uses state abbreviations |
| `renderCountyMap()` | County, CBSA, CSA, CZ | **D3.js** | Manual SVG rendering with `d3.geoAlbersUsa()`, county GeoJSON, quantile color scale |

**This dual-renderer architecture is the root cause of inconsistent map behavior.**

### 2.2 State Map (Plotly) — How It Works

```
Data → filter by state FIPS → lookup state abbreviation → Plotly choropleth
```
- Uses Plotly's built-in US state geometry
- State abbreviation lookup via `stateFipsToAbbr` map (built from crosswalk)
- Percentile-clipped color scale
- Interactive hover/zoom via Plotly

### 2.3 County/Sub-state Map (D3) — How It Works

```
Data → build valMap (countyFIPS → value) → load us-counties-2023.json
     → D3 geoAlbersUsa projection → SVG path rendering
     → overlay state boundaries from us-atlas TopoJSON CDN
```
- For CBSA/CSA/CZ levels: maps parent region values DOWN to their constituent counties via `countyToParent` lookup, so the map still renders at county granularity but colors counties by their parent region's value
- Uses `d3.scaleQuantile()` with 5 bins (not continuous)
- Custom SVG legend (not Plotly's built-in colorbar)
- Manual tooltip via DOM manipulation
- State boundaries loaded from `cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json`

### 2.4 Current Map Limitations

**A. No Zoom/Pan on County Maps**
The D3 county maps are static SVGs. Unlike the Plotly state maps (which have built-in zoom/pan), the D3 maps have no interactivity beyond hover tooltips. Users cannot zoom into a region of interest.

**B. No Click-to-Select on Maps**
Neither map type supports clicking a region to navigate to its Regional View. This is a missed opportunity for cross-tab navigation.

**C. State Filter Doesn't Zoom the Map**
When you filter counties by state in the National View, the map still shows the entire US — it just leaves non-matching counties white. There's no zoom-to-extent when filtering.

**D. Inconsistent Color Scale Approaches**
- State maps: continuous Plotly colorscale with percentile clipping
- County maps: 5-bin quantile D3 scale
- This means the same metric looks visually different depending on the geographic level

**E. No CBSA/CSA/CZ Boundary Lines**
When viewing CBSA, CSA, or CZ data, the map colors individual counties but doesn't draw the actual region boundaries. Users can't visually distinguish where one CBSA ends and another begins.

**F. External CDN Dependency for State Boundaries**
State boundary overlay depends on `cdn.jsdelivr.net`. If that CDN is down or blocked, state lines silently fail (caught in a try/catch). This is fragile for a production tool.

**G. Legend Positioning Issues**
The D3 map legend is positioned to the right using a fixed `legendGap = 160`. On narrow viewports, this can clip or overlap the map.

**H. No Alaska/Hawaii Inset Handling**
Both map renderers use `geoAlbersUsa` which includes Alaska and Hawaii as insets. This works but the small county sizes in these states make them effectively unusable for county-level analysis.

**I. Missing "No Data" Distinction**
Counties with no data render as white (`#ffffff`), which looks identical to the page background. There's no visual distinction between "no data" and "not part of the map."

**J. 3.4 MB GeoJSON Loaded Every Time**
The county GeoJSON file is 3.4 MB. It's fetched and parsed on every county-level map render (though cached in-memory after first load). Initial load is noticeable.

---

## Part 3: Systematic Map Improvement Plan

### Priority 1 — Make Maps Interactive (High Impact, Moderate Effort)

#### 1A. Add Zoom/Pan to County Maps

**Current state:** Static SVG, no interaction beyond hover.

**Recommended approach:** Add D3 zoom behavior to the SVG container.

```javascript
// Add to renderCountyMap() after creating the SVG
const zoom = d3.zoom()
  .scaleExtent([1, 12])
  .on('zoom', (event) => {
    svg.selectAll('g').attr('transform', event.transform);
    // Update state boundary paths too
    svg.selectAll('path.state-boundary')
      .attr('stroke-width', 0.8 / event.transform.k);
  });
svg.call(zoom);
```

**Effort:** ~30 lines of code. Need to also add a reset-zoom button.

#### 1B. Auto-Zoom When State Filter Is Applied

When a user selects a state in the county-level National View, zoom the map to that state's bounding box.

```javascript
// After filtering counties by state, compute bounds and zoom
const stateBounds = d3.geoPath(projection).bounds(stateFeature);
const dx = stateBounds[1][0] - stateBounds[0][0];
const dy = stateBounds[1][1] - stateBounds[0][1];
// Apply zoom transform to center on state
```

**Effort:** ~40 lines of code.

#### 1C. Click-to-Navigate Between Map and Regional View

When a user clicks a county/state/region on the National View map, switch to the Regional View with that geography pre-selected.

```javascript
.on('click', function(event, d) {
  const geoid = d.id;
  // Switch to Regional tab
  document.querySelector('[data-tab="regional"]').click();
  // Set the geography and trigger load
  document.getElementById('reg-level').value = level;
  populateRegGeoList();
  regGeoWidget.setValue(geoid, displayName);
  loadRegional(level, geoid);
});
```

**Effort:** ~25 lines of code.

### Priority 2 — Visual Consistency (Medium Impact, Low Effort)

#### 2A. Unify Color Scale Approach

Use the same color encoding strategy for both state and county maps. Two options:

**Option A (Recommended): Use quantile bins everywhere.** This provides maximum visual contrast and works well for skewed distributions (which economic data almost always has). Update `renderStateMap()` to use a D3 quantile scale instead of Plotly's continuous scale.

**Option B:** Switch county maps to Plotly's `choropleth` with `locationmode: 'geojson-id'`, unifying on Plotly. This gives consistent hover/zoom but Plotly's county rendering can be slower.

#### 2B. Add Region Boundary Overlays for CBSA/CSA/CZ

When viewing CBSA-level data, dissolve county boundaries within each CBSA and draw the CBSA outline. This requires building merged boundary polygons, which can be done with TopoJSON's `merge` function.

```javascript
// For each unique parent region, collect its counties and merge
const regionGroups = d3.group(geojson.features.filter(f => valMap.has(f.id)),
  f => parentLookup(f.id));
regionGroups.forEach((counties, regionId) => {
  // Draw thicker boundary around grouped counties
});
```

**Alternatively:** Use the geoparquet/gpkg files from the release to generate CBSA/CSA/CZ GeoJSON boundary files at build time, then load those directly instead of deriving them from county boundaries at render time. This is cleaner and faster.

**Effort:** Either ~60 lines of runtime code, or a one-time build script + ~20 lines of rendering code.

#### 2C. Fix "No Data" Styling

Give counties with no data a distinct fill (light gray with a subtle pattern or crosshatch) instead of white.

```javascript
.attr('fill', d => {
  const val = valMap.get(d.id);
  return val != null ? color(val) : '#f0f0f0';  // Light gray, not white
})
```

**Effort:** 1 line change + optional crosshatch pattern (~10 lines).

### Priority 3 — Performance (Medium Impact, Moderate Effort)

#### 3A. Pre-simplify and Compress GeoJSON

The `us-counties-2023.json` file is 3.4 MB of raw GeoJSON. You can reduce this significantly:

1. **Convert to TopoJSON** — Typically 60-80% smaller than GeoJSON due to shared arc encoding. You already load TopoJSON-client from CDN.
2. **Simplify geometry** — Use `topojson-simplify` to reduce vertex count while preserving visual fidelity at the dashboard's zoom levels. A simplification of `1e-7` typically reduces file size by 50%+ with minimal visual impact.
3. **Gzip the result** — Since you already have Pako for CSV decompression, serve the topology as `.json.gz` and decompress client-side.

**Expected result:** 3.4 MB → ~400-600 KB (gzipped TopoJSON).

```bash
# Build-time conversion
npx topojson-server counties.json -o topo.json
npx topojson-simplify topo.json -p 1e-7 -o topo-simple.json
gzip topo-simple.json
```

**Effort:** Build script changes + ~15 lines of JS to switch from GeoJSON to TopoJSON loading.

#### 3B. Bundle State Boundaries Locally

Replace the CDN fetch for state boundaries (`cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json`) with a local copy in `/docs`. This eliminates the external dependency and the extra HTTP request.

**Effort:** Copy one file + change one URL.

#### 3C. Lazy-Load Map Data

Currently, `initNational()` pre-loads metadata and auto-triggers `loadNational()` with Battery Manufacturing (335910) pre-selected. Consider deferring the initial map render until the user actually navigates to the National tab, since the Regional tab might be their primary use case.

**Effort:** ~10 lines of code change.

### Priority 4 — UX Enhancements (High Impact, Higher Effort)

#### 4A. Add a Mini-Map or Locator for Regional View

When a user selects a specific county in the Regional View, show a small locator map highlighting that county's position within the US (or within its state). This provides geographic context that's currently missing entirely from the Regional View.

**Effort:** ~80-100 lines of code.

#### 4B. Add Comparison Mode

Allow users to select 2-3 geographies and compare them side-by-side in the Regional View. This is one of the most commonly requested features in economic development tools.

**Effort:** Significant refactor of Regional View — probably 200+ lines.

#### 4C. Deep-Link Support (URL Hash Routing)

Allow sharing specific views via URL. For example: `#/national/county/335910/industry_feasibility` or `#/regional/county/01001`. This makes the tool citable and shareable.

```javascript
// On load, parse hash
const hash = window.location.hash;
if (hash.startsWith('#/national/')) { /* parse and set state */ }

// On state change, update hash
window.location.hash = `/national/${level}/${indCode}/${metric}`;
```

**Effort:** ~60-80 lines of code.

#### 4D. Search-on-Map

Add a "Find on Map" button next to the geography search that highlights and zooms to the selected geography on the National View map.

**Effort:** ~30 lines of code (requires zoom implementation from 1A).

---

## Part 4: Code Architecture Recommendations

### 4.1 Split `index.html` into Modules

The entire dashboard is a single 1,970-line HTML file. This makes maintenance difficult and prevents effective code review. Consider splitting into:

```
docs/
  index.html          (HTML structure only)
  css/
    styles.css        (all styles)
  js/
    config.js         (constants, palettes, metric descriptions)
    utils.js          (fetch, format, padGeoid, spinner)
    widgets.js        (SearchSelect, SortableTable, ExportBar)
    maps.js           (renderStateMap, renderCountyMap, map helpers)
    regional.js       (Regional View tab)
    national.js       (National View tab)
    industry-space.js (Industry Space tab)
    app.js            (init, tab switching, wiring)
```

This does NOT require a build tool — you can use `<script type="module">` with ES imports, which all modern browsers support. Or even just multiple `<script>` tags in the right order.

**Benefits:** Code review on PRs becomes tractable, map code can be iterated independently, and you can add tests.

### 4.2 Add Error Boundaries per Tab

Currently, an error in any tab's init function can prevent other tabs from loading (they're awaited sequentially in `init()`). Wrap each in a try/catch:

```javascript
try { await initRegional(); } catch(e) { console.error('Regional init failed:', e); }
try { await initNational(); } catch(e) { console.error('National init failed:', e); }
try { await initIndustrySpace(); } catch(e) { console.error('Industry Space init failed:', e); }
```

### 4.3 Add a Build Step for Data Validation

The `split_release_data.py` script splits data but doesn't validate the output. Add checks for:
- All expected FIPS codes present
- No NaN values in required columns
- File counts match expected geography counts
- Column schema matches what `index.html` expects

---

## Part 5: Recommended Implementation Order

| # | Task | Impact | Effort | Dependencies |
|---|------|--------|--------|-------------|
| 1 | Fix "no data" fill color | Quick win | 1 line | None |
| 2 | Bundle state boundaries locally | Reliability | Copy 1 file | None |
| 3 | Add zoom/pan to D3 county maps | High | ~30 lines | None |
| 4 | Add error boundaries per tab | Reliability | ~10 lines | None |
| 5 | Click-to-navigate from map to Regional View | High | ~25 lines | #3 |
| 6 | Auto-zoom on state filter | Medium | ~40 lines | #3 |
| 7 | Unify color scale approach | Medium | ~50 lines | None |
| 8 | Convert GeoJSON to TopoJSON | Performance | Build script + ~15 lines | None |
| 9 | Add CBSA/CSA/CZ boundary overlays | Medium | ~60 lines | None |
| 10 | Split index.html into modules | Maintainability | Refactor | None |
| 11 | Add mini-map locator to Regional View | Medium | ~100 lines | None |
| 12 | URL hash routing / deep links | High | ~70 lines | None |
| 13 | Comparison mode | High | ~200+ lines | #10 |

**Suggested sprints:**
- **Sprint 1 (quick wins):** Items 1-4 — can be done in a single PR
- **Sprint 2 (map interactivity):** Items 5-7 — makes maps actually useful
- **Sprint 3 (performance + boundaries):** Items 8-9 — polish
- **Sprint 4 (architecture + UX):** Items 10-13 — longer-term

---

## Part 6: Repo Hygiene Notes

1. **`.webarchive` files at root** — These are Safari web archive files used as developer references. They're not used by the dashboard and bloat the repo. Consider moving to a `references/` folder or removing entirely.

2. **EPS logo files at root** — `RMI_logo_PrimaryUse.eps` and the white horizontal variant are print-format logos not used by the web dashboard. Could be moved to an `assets/print/` folder.

3. **README is minimal** — The README says materials are "not yet vetted." Consider expanding it with: setup instructions, data pipeline documentation, architecture overview, and contribution guidelines.

4. **No `.gitignore` for common files** — The existing `.gitignore` handles large data files but consider adding entries for `.DS_Store`, `node_modules/`, `__pycache__/`, etc.

5. **No tests** — There are no unit tests for any JavaScript logic (padGeoid, color scales, data parsing) or for the Python data pipeline. Even basic smoke tests would catch regressions.

6. **Release data redundancy** — The `dashboard_data_20260308_204924/` folder at root appears to be an intermediate output from `split_release_data.py` that's also in `docs/`. Consider `.gitignore`-ing it or documenting its purpose.
