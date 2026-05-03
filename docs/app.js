
/* ============================================================================
   RMI Clean Growth Tool — Dashboard
   v2 performance-optimized boot path:
     - Single bootstrap.json fetch on load (was: 6+ sequential fetches)
     - Pre-computed national maps (was: fan-out fetch of 20+ industry slices)
     - Lazy heavy sections: industry space + colocation only on user request
     - performance.mark / performance.measure throughout
     - pako only used for the few .csv.gz files (peer, edges, colocation)
   ============================================================================ */

(function () {
  'use strict';
  const PERF = (label, fn) => {
    const start = label + ':start', end = label + ':end';
    performance.mark(start);
    const result = fn();
    if (result && typeof result.then === 'function') {
      return result.finally(() => {
        performance.mark(end);
        try { performance.measure(label, start, end); } catch (e) {}
      });
    }
    performance.mark(end);
    try { performance.measure(label, start, end); } catch (e) {}
    return result;
  };
  const TIME = (label, fn) => {
    console.time(label);
    const result = fn();
    if (result && typeof result.then === 'function') {
      return result.finally(() => console.timeEnd(label));
    }
    console.timeEnd(label);
    return result;
  };

  /* ---- boot detail line ---------------------------------------------------- */
  function setBootDetail(text) {
    const el = document.getElementById('boot-detail');
    if (el) el.textContent = text;
  }
  function showBootError(err) {
    const el = document.getElementById('boot-screen');
    if (el) {
      el.innerHTML = '<div class="boot-card"><div class="error-box">' +
        '<strong>Failed to load.</strong><br>' +
        (err && err.message ? err.message : err) +
        '</div></div>';
    }
  }

  /* ---- fetcher ------------------------------------------------------------ */
  async function fetchJSON(url) {
    return TIME('fetch:' + url, async () => {
      const res = await fetch(url, { cache: 'force-cache' });
      if (!res.ok) throw new Error('HTTP ' + res.status + ' for ' + url);
      return res.json();
    });
  }

  // Plain CSV (UTF-8); GitHub Pages handles HTTP gzip transparently.
  async function fetchCSV(url) {
    return TIME('fetch:' + url, async () => {
      const res = await fetch(url, { cache: 'force-cache' });
      if (!res.ok) throw new Error('HTTP ' + res.status + ' for ' + url);
      const text = await res.text();
      return d3.csvParse(text);
    });
  }

  // .csv.gz files (peer geography, industry space edges, colocation) — these
  // are double-served as binary gzip; we decompress with pako client-side.
  async function fetchCSVGZ(url) {
    return TIME('fetch:' + url, async () => {
      const res = await fetch(url, { cache: 'force-cache' });
      if (!res.ok) throw new Error('HTTP ' + res.status + ' for ' + url);
      const buf = await res.arrayBuffer();
      let text;
      try {
        text = pako.inflate(new Uint8Array(buf), { to: 'string' });
      } catch (e) {
        // Some servers may have already decompressed; try as plain text.
        text = new TextDecoder().decode(buf);
      }
      return d3.csvParse(text);
    });
  }

  async function fetchGeoJSONGZ(url) {
    return TIME('fetch:' + url, async () => {
      const res = await fetch(url, { cache: 'force-cache' });
      if (!res.ok) throw new Error('HTTP ' + res.status + ' for ' + url);
      const buf = await res.arrayBuffer();
      let text;
      try {
        text = pako.inflate(new Uint8Array(buf), { to: 'string' });
      } catch (e) {
        text = new TextDecoder().decode(buf);
      }
      return JSON.parse(text);
    });
  }

  /* ---- state -------------------------------------------------------------- */
  const State = {
    bootstrap: null,
    config: null,
    geographies: null,
    industries: null,
    energyTechCrosswalk: null,
    energyTechCategories: null,
    diagnostics: null,
    // caches keyed by URL
    cacheGeoJSON: {},
    cacheMaps: {},
    cacheFactByGeography: {},
    cacheFactByIndustry: {},
    // ui state
    currentLevel: 'cz',
    currentMetric: 'industry_feasibility',
    currentSubmetric: 'percentile',
    currentSelector: { type: 'naics6', code: null }, // or { type: 'tech', key: 'Solar | supply chain' }
  };
  window.AppState = State;

  /* ---- tooltip ----------------------------------------------------------- */
  const tooltip = document.getElementById('tooltip');
  function tt(html, evt) {
    tooltip.innerHTML = html;
    tooltip.hidden = false;
    const x = evt.clientX + 14, y = evt.clientY + 12;
    const w = tooltip.offsetWidth, h = tooltip.offsetHeight;
    const xx = (x + w + 12 > window.innerWidth) ? evt.clientX - w - 14 : x;
    const yy = (y + h + 12 > window.innerHeight) ? evt.clientY - h - 12 : y;
    tooltip.style.left = xx + 'px';
    tooltip.style.top = yy + 'px';
  }
  function ttHide() { tooltip.hidden = true; }
  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }
  function makeClickableRow(onActivate, opts, ...cells) {
    const row = el('tr', Object.assign({
      role: 'button',
      tabindex: '0',
      onclick: onActivate,
      onkeydown: (event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          onActivate();
        }
      }
    }, opts || {}), ...cells);
    return row;
  }


  /* ---- boot --------------------------------------------------------------- */
  async function boot() {
    const bootStart = performance.now();
    try {
      setBootDetail('Fetching bootstrap bundle...');
      const buildVersion = document.getElementById('footer-pipeline-id')?.textContent || 'current';
      const bs = await PERF('boot:bootstrap', () =>
        fetchJSON('data/bootstrap.json?v=' + encodeURIComponent(buildVersion)));
      State.bootstrap = bs;
      State.config = bs.config;
      State.geographies = bs.geographies;
      State.industries = bs.industries;
      State.energyTechCrosswalk = bs.energy_tech_crosswalk;
      State.energyTechCategories = bs.energy_tech_categories;
      State.diagnostics = bs.diagnostics;

      // Build helper indexes
      State.industriesByCode = {};
      (State.industries || []).forEach(d => {
        State.industriesByCode[d.industry_code] = d;
      });
      State.techByKey = {};
      (State.energyTechCategories || []).forEach(p => {
        const k = p.energy_tech_category + ' | ' + p.energy_tech_subcategory;
        State.techByKey[k] = p;
      });

      setBootDetail('Bootstrap ready (' +
        Object.keys(State.geographies).length + ' geographic levels, ' +
        (State.industries || []).length + ' industries, ' +
        (State.energyTechCategories || []).length + ' tech categories)');

      document.getElementById('boot-screen').hidden = true;
      document.getElementById('view').hidden = false;

      // Footer + nav
      document.getElementById('footer-pipeline-id').textContent = bs.pipeline_id;
      document.getElementById('footer-build-time').textContent = bs.build_time;

      window.addEventListener('hashchange', router);
      router();

      const dt = (performance.now() - bootStart).toFixed(0);
      console.log('[boot] complete in ' + dt + ' ms');
    } catch (err) {
      console.error('[boot] failed', err);
      showBootError(err);
    }
  }

  /* ---- router ------------------------------------------------------------- */
  function router() {
    const hash = window.location.hash || '#/';
    const parts = hash.replace(/^#\/?/, '').split('/').filter(Boolean);
    const root = parts[0] || 'home';
    document.querySelectorAll('.site-nav a').forEach(a => {
      a.classList.toggle('active', a.dataset.route === root ||
        (root === '' && a.dataset.route === 'home'));
    });
    PERF('route:' + root, () => {
      if (root === '' || root === 'home') return renderHome();
      if (root === 'national') return renderNational();
      if (root === 'regional' && parts.length === 1) return renderRegionalIndex();
      if (root === 'regional' && parts.length === 3)
        return renderRegional(parts[1], parts[2]);
      if (root === 'about') return renderAbout();
      return renderNotFound(hash);
    });
  }
  function navTo(path) {
    if (window.location.hash !== path) window.location.hash = path;
    else router();
  }

  /* ---- helpers ------------------------------------------------------------ */
  const view = () => document.getElementById('view');
  function clearView() { view().innerHTML = ''; }
  function el(tag, opts, ...children) {
    const e = document.createElement(tag);
    if (opts) for (const [k, v] of Object.entries(opts)) {
      if (k === 'class') e.className = v;
      else if (k === 'html') e.innerHTML = v;
      else if (k === 'style') Object.assign(e.style, v);
      else if (k.startsWith('on') && typeof v === 'function')
        e.addEventListener(k.slice(2).toLowerCase(), v);
      else e.setAttribute(k, v);
    }
    children.flat().forEach(c => {
      if (c == null) return;
      if (typeof c === 'string') e.appendChild(document.createTextNode(c));
      else e.appendChild(c);
    });
    return e;
  }

  function levelMeta(key) {
    const m = (State.config.geographic_levels || []).find(g => g.key === key);
    return m || { key, display_name: key, supports_industry_space: false };
  }
  function metricMeta(key) {
    return (State.config.metrics || []).find(m => m.key === key) || { key, display_name: key };
  }

  function fmtNum(x, d) {
    if (x == null || isNaN(x)) return '—';
    return Number(x).toLocaleString(undefined,
      { minimumFractionDigits: d || 0, maximumFractionDigits: d || 0 });
  }
  function fmtScore(x) { return x == null ? '—' : Number(x).toFixed(2); }

  /* ---- home --------------------------------------------------------------- */
  function renderHome() {
    clearView();
    const v = view();
    const cfg = State.config;

    const intro = el('div', { class: 'section' },
      el('h2', null, 'Clean Growth Tool'),
      el('div', { class: 'section-sub' },
        'Industry-level economic complexity for U.S. counties, states, ' +
        'metro areas, combined statistical areas, and commuting zones.'),
      el('div', null,
        'Use the National view to explore feasibility and existing concentration ' +
        'across the country, or jump straight to a region.')
    );
    v.appendChild(intro);

    // Quick stats
    const stats = el('div', { class: 'kpi-grid' });
    State.diagnostics.forEach(d => {
      const card = el('div', { class: 'kpi' },
        el('div', { class: 'kpi-label' }, d.geo_aggregation_name.toUpperCase()),
        el('div', { class: 'kpi-value' }, fmtNum(d.n_geographies)),
        el('div', { class: 'kpi-detail' },
          d.n_industries + ' industries · fill ' +
          fmtNum(d.fill_rate_pct, 1) + '%')
      );
      stats.appendChild(card);
    });
    v.appendChild(el('div', { class: 'section' },
      el('h3', null, 'Coverage'), stats));

    // Quick navigation cards
    const nav = el('div', { class: 'section' },
      el('h3', null, 'Jump in'),
      el('div', { class: 'row-2' },
        el('div', null,
          el('h3', null, 'National'),
          el('p', null, 'Country-wide choropleth and feasibility scatter for any industry or technology.'),
          el('a', { href: '#/national', class: 'tag-pill' }, 'Open national view →')),
        el('div', null,
          el('h3', null, 'Regional'),
          el('p', null, 'Pick a region (county, metro, state, etc.) to see its peer geographies, top industries, and detail charts.'),
          el('a', { href: '#/regional', class: 'tag-pill' }, 'Open regional view →')))
    );
    v.appendChild(nav);
  }

  /* ---- about -------------------------------------------------------------- */
  function renderAbout() {
    clearView();
    const v = view();
    const cfg = State.config;
    const prov = cfg.provenance || {};
    v.appendChild(el('div', { class: 'section' },
      el('h2', null, 'About'),
      el('div', { class: 'section-sub' },
        'Methodology, sources, and build provenance.'),
      el('h3', null, 'Sources'),
      el('ul', null,
        el('li', null, 'Lightcast 2024 employment by 6-digit NAICS by county (file: ' + (prov.lightcast_file || '—') + ')'),
        el('li', null, 'TIGRIS ' + (prov.tigris_year || '') + ' geometries (counties, states, CBSA, CSA)'),
        el('li', null, 'USDA Commuting Zones ' + (prov.commuting_zones_vintage || '')),
        el('li', null, 'Energy-tech crosswalk: ' + (prov.energy_tech_crosswalk_file || '—'))),
      el('h3', null, 'Methods'),
      el('p', null,
        'Economic complexity is calculated using the eigenvector method ' +
        '(Daboin et al.). The Industry Complexity Index and Economic Complexity Index ' +
        'are derived from the second eigenvector of the row-stochastic specialization ' +
        'matrix M̃. Feasibility (industry density) is computed from the proximity matrix.'),
      el('h3', null, 'Build'),
      el('p', null, 'Pipeline: ', el('code', null, cfg.pipeline_id)),
      el('p', null, 'Built: ', cfg.build_time)
    ));
  }

  function renderNotFound(hash) {
    clearView();
    view().appendChild(el('div', { class: 'section' },
      el('h2', null, 'Not found'),
      el('div', { class: 'error-box' }, 'No route matches ' + hash)
    ));
  }


  /* ---- choropleth helpers ------------------------------------------------- */
  async function loadGeoJSON(level) {
    if (State.cacheGeoJSON[level]) return State.cacheGeoJSON[level];
    setBootDetail('Loading ' + level + ' map...');
    const gj = await PERF('geojson:' + level, () =>
      fetchGeoJSONGZ('data/geo/' + level + '.geojson.gz'));
    State.cacheGeoJSON[level] = gj;
    return gj;
  }

  async function loadMap(level, metric, sub) {
    const fname = level + '_' + metric + '_' + sub + '.json';
    if (State.cacheMaps[fname]) return State.cacheMaps[fname];
    const data = await PERF('map:' + fname, () =>
      fetchJSON('data/maps/' + fname));
    State.cacheMaps[fname] = data;
    return data;
  }

  async function loadEcMap(level) {
    const fname = level + '_economic_complexity.json';
    if (State.cacheMaps[fname]) return State.cacheMaps[fname];
    const data = await PERF('map:' + fname, () =>
      fetchJSON('data/maps/' + fname));
    State.cacheMaps[fname] = data;
    return data;
  }

  // Get a {geoid -> value} map for the current selector + metric.
  async function resolveMap(level, metric, sub, selector) {
    if (metric === 'economic_complexity') {
      const m = await loadEcMap(level);
      return sub === 'percentile'
        ? m.economic_complexity_percentile
        : m.economic_complexity_raw;
    }
    const all = await loadMap(level, metric, sub);
    if (selector.type === 'naics6') {
      return all['naics6:' + selector.code] || {};
    }
    if (selector.type === 'tech') {
      return all['tech:' + selector.key] || {};
    }
    return {};
  }

  function geoidProperty(level) {
    return ({
      county: 'county_geoid',
      state: 'state_fips',
      cbsa: 'cbsa_geoid',
      csa: 'csa_geoid',
      cz: 'commuting_zone_geoid',
    })[level];
  }

  function colorScale(values, palette) {
    const vals = values.filter(v => v != null && !isNaN(v));
    if (!vals.length) return () => '#ebeef2';
    const ext = d3.extent(vals);
    const range = palette || State.config.palette.sequential_5;
    const stops = range.length;
    const scale = d3.scaleQuantize().domain(ext).range(range);
    scale.colorRange = range;
    scale.extent = ext;
    return scale;
  }

  function renderChoropleth(svgEl, geojson, valuesByGeoid, level, palette, onClick) {
    const svg = d3.select(svgEl);
    svg.selectAll('*').remove();
    const w = svgEl.clientWidth, h = svgEl.clientHeight;
    const projection = d3.geoAlbersUsa().fitSize([w, h], geojson);
    const path = d3.geoPath(projection);
    const idProp = geoidProperty(level);

    const vals = Object.values(valuesByGeoid).filter(v => v != null && !isNaN(v));
    const scale = colorScale(vals, palette);

    const g = svg.append('g');
    g.selectAll('path').data(geojson.features).enter().append('path')
      .attr('class', d => {
        const v = valuesByGeoid[d.properties[idProp]];
        return 'geo' + (v == null ? ' no-data' : '');
      })
      .attr('d', path)
      .attr('fill', d => {
        const v = valuesByGeoid[d.properties[idProp]];
        return v == null ? '#ebeef2' : scale(v);
      })
      .on('mousemove', (event, d) => {
        const v = valuesByGeoid[d.properties[idProp]];
        const name = d.properties.name || d.properties.county_name ||
          d.properties.state_name || d.properties.cbsa_name ||
          d.properties.csa_name || d.properties.commuting_zone_name ||
          d.properties[idProp];
        tt('<strong>' + escapeHtml(name || '—') + '</strong><br>' +
          'Value: ' + escapeHtml(v == null ? '—' : fmtScore(v)), event);
      })
      .on('mouseleave', ttHide)
      .on('click', (event, d) => {
        if (onClick) onClick(d.properties[idProp], d);
      });

    return scale;
  }

  function renderLegend(container, scale) {
    container.innerHTML = '';
    if (!scale.colorRange) {
      container.appendChild(document.createTextNode('No data'));
      return;
    }
    const [lo, hi] = scale.extent;
    const bar = el('div', { class: 'legend-bar' });
    scale.colorRange.forEach(c => {
      bar.appendChild(el('div', { style: { background: c } }));
    });
    container.appendChild(el('span', null, fmtScore(lo)));
    container.appendChild(bar);
    container.appendChild(el('span', null, fmtScore(hi)));
  }

  /* ---- national view ------------------------------------------------------ */
  async function renderNational() {
    clearView();
    const v = view();

    // Controls
    const ctrls = el('div', { class: 'controls' });
    const levelSel = el('select', { id: 'nat-level' });
    State.config.geographic_levels.forEach(L => {
      levelSel.appendChild(el('option', { value: L.key }, L.display_name));
    });
    levelSel.value = State.currentLevel;
    levelSel.addEventListener('change', () => {
      State.currentLevel = levelSel.value;
      drawNational();
    });
    ctrls.appendChild(el('div', { class: 'control' },
      el('label', null, 'Geographic level'), levelSel));

    const metricSel = el('select', { id: 'nat-metric' });
    State.config.metrics.forEach(M => {
      metricSel.appendChild(el('option', { value: M.key }, M.display_name));
    });
    metricSel.value = State.currentMetric;
    const subSel = el('select', { id: 'nat-sub' });
    function refreshSub() {
      subSel.innerHTML = '';
      const m = metricMeta(metricSel.value);
      m.submetrics.forEach(s => {
        subSel.appendChild(el('option', { value: s }, s.replace(/_/g, ' ')));
      });
      // pick a reasonable default
      if (metricSel.value === 'economic_complexity') subSel.value = 'percentile';
      else if (metricSel.value === 'industry_feasibility') subSel.value = 'percentile';
      else subSel.value = 'location_quotient';
      State.currentSubmetric = subSel.value;
    }
    refreshSub();
    metricSel.addEventListener('change', () => {
      State.currentMetric = metricSel.value;
      refreshSub();
      State.currentSubmetric = subSel.value;
      toggleSelectorVisibility();
      drawNational();
    });
    subSel.addEventListener('change', () => {
      State.currentSubmetric = subSel.value;
      drawNational();
    });
    ctrls.appendChild(el('div', { class: 'control' },
      el('label', null, 'Metric'), metricSel));
    ctrls.appendChild(el('div', { class: 'control' },
      el('label', null, 'Submetric'), subSel));

    // Industry / tech selector
    const aggSel = el('select', { id: 'nat-agg' });
    State.config.industry_aggregations.forEach(A => {
      aggSel.appendChild(el('option', { value: A.key }, A.display_name));
    });
    aggSel.value = State.currentSelector.type;
    const naicsSel = el('select', { id: 'nat-naics' });
    (State.industries || []).forEach(ind => {
      naicsSel.appendChild(el('option', { value: ind.industry_code },
        ind.industry_code + ' — ' + ind.industry_description));
    });
    if (!State.currentSelector.code && State.industries.length) {
      State.currentSelector.code = State.industries[0].industry_code;
    }
    naicsSel.value = State.currentSelector.code || '';
    const techSel = el('select', { id: 'nat-tech' });
    (State.energyTechCategories || []).forEach(p => {
      const k = p.energy_tech_category + ' | ' + p.energy_tech_subcategory;
      techSel.appendChild(el('option', { value: k }, k));
    });
    if (State.energyTechCategories && State.energyTechCategories.length) {
      const first = State.energyTechCategories[0];
      techSel.value = first.energy_tech_category + ' | ' + first.energy_tech_subcategory;
    }

    aggSel.addEventListener('change', () => {
      State.currentSelector.type = aggSel.value;
      toggleSelectorVisibility();
      drawNational();
    });
    naicsSel.addEventListener('change', () => {
      State.currentSelector.type = 'naics6';
      State.currentSelector.code = naicsSel.value;
      drawNational();
    });
    techSel.addEventListener('change', () => {
      State.currentSelector.type = 'tech';
      State.currentSelector.key = techSel.value;
      drawNational();
    });

    const naicsCtrl = el('div', { class: 'control' },
      el('label', null, 'NAICS-6 industry'), naicsSel);
    const techCtrl = el('div', { class: 'control' },
      el('label', null, 'Energy technology'), techSel);
    ctrls.appendChild(el('div', { class: 'control' },
      el('label', null, 'Industry aggregation'), aggSel));
    ctrls.appendChild(naicsCtrl);
    ctrls.appendChild(techCtrl);

    function toggleSelectorVisibility() {
      const isEcon = metricSel.value === 'economic_complexity';
      aggSel.disabled = isEcon;
      naicsCtrl.style.display = !isEcon && aggSel.value === 'naics6' ? '' : 'none';
      techCtrl.style.display = !isEcon && aggSel.value === 'tech' ? '' : 'none';
    }
    toggleSelectorVisibility();

    const section = el('div', { class: 'section' },
      el('h2', null, 'National view'),
      el('div', { class: 'section-sub' },
        'Choropleth across all U.S. geographies for the selected metric.'),
      ctrls,
      el('div', { class: 'choropleth-wrap' },
        el('svg', { class: 'choropleth', id: 'nat-choro' })),
      el('div', { class: 'legend', id: 'nat-legend' })
    );
    v.appendChild(section);

    // Scatter section: ECI vs feasibility
    const scatterSection = el('div', { class: 'section' },
      el('h3', null, 'Economic Complexity vs Feasibility'),
      el('div', { class: 'section-sub' },
        'Each point is one geography. X = ECI percentile, Y = mean feasibility (selected industry/tech).'),
      el('svg', { class: 'scatter', id: 'nat-scatter' })
    );
    v.appendChild(scatterSection);

    drawNational();
  }

  async function drawNational() {
    const level = State.currentLevel;
    const metric = State.currentMetric;
    const sub = State.currentSubmetric;
    const sel = State.currentSelector;
    try {
      const [gj, valMap] = await Promise.all([
        loadGeoJSON(level),
        resolveMap(level, metric, sub, sel),
      ]);
      const svgEl = document.getElementById('nat-choro');
      const scale = renderChoropleth(svgEl, gj, valMap, level,
        State.config.palette.sequential_7,
        (id) => navTo('#/regional/' + level + '/' + id));
      renderLegend(document.getElementById('nat-legend'), scale);

      // Scatter: ECI percentile (x) vs map value (y)
      const ec = await loadEcMap(level);
      drawNationalScatter(ec, valMap, level);
    } catch (err) {
      console.error(err);
      view().appendChild(el('div', { class: 'error-box' },
        'Failed to load national view: ' + err.message));
    }
  }

  function drawNationalScatter(ec, valMap, level) {
    const svgEl = document.getElementById('nat-scatter');
    const svg = d3.select(svgEl);
    svg.selectAll('*').remove();
    const w = svgEl.clientWidth, h = svgEl.clientHeight;
    const margin = { top: 16, right: 16, bottom: 42, left: 52 };
    const innerW = w - margin.left - margin.right;
    const innerH = h - margin.top - margin.bottom;
    const g = svg.append('g').attr('transform',
      'translate(' + margin.left + ',' + margin.top + ')');

    const points = [];
    Object.keys(ec.economic_complexity_percentile || {}).forEach(gid => {
      const x = ec.economic_complexity_percentile[gid];
      const y = valMap[gid];
      if (x != null && y != null && !isNaN(x) && !isNaN(y))
        points.push({ gid, x: +x, y: +y });
    });

    if (!points.length) {
      g.append('text').attr('x', innerW / 2).attr('y', innerH / 2)
        .attr('text-anchor', 'middle').attr('class', 'axis')
        .text('No data available for this combination');
      return;
    }

    const xExt = d3.extent(points, p => p.x);
    const yExt = d3.extent(points, p => p.y);
    const xs = d3.scaleLinear().domain(xExt).nice().range([0, innerW]);
    const ys = d3.scaleLinear().domain(yExt).nice().range([innerH, 0]);

    g.append('g').attr('class', 'axis')
      .attr('transform', 'translate(0,' + innerH + ')')
      .call(d3.axisBottom(xs).ticks(6));
    g.append('g').attr('class', 'axis').call(d3.axisLeft(ys).ticks(6));

    g.append('text').attr('class', 'axis-label')
      .attr('x', innerW / 2).attr('y', innerH + 36)
      .attr('text-anchor', 'middle').text('Economic Complexity (percentile)');
    g.append('text').attr('class', 'axis-label')
      .attr('transform', 'rotate(-90)')
      .attr('x', -innerH / 2).attr('y', -38)
      .attr('text-anchor', 'middle').text(State.currentMetric.replace(/_/g, ' '));

    const dotColor = State.config.palette.scatter_tiers.specialized;
    g.selectAll('circle').data(points).enter().append('circle')
      .attr('cx', d => xs(d.x)).attr('cy', d => ys(d.y))
      .attr('r', 3).attr('fill', dotColor).attr('opacity', 0.6)
      .on('mousemove', (e, d) => tt(
        '<strong>' + d.gid + '</strong><br>' +
        'ECI %ile: ' + fmtScore(d.x) + '<br>' +
        'Y value: ' + fmtScore(d.y), e))
      .on('mouseleave', ttHide)
      .on('click', (e, d) => navTo('#/regional/' + State.currentLevel + '/' + d.gid));
  }


  /* ---- regional index ----------------------------------------------------- */
  function renderRegionalIndex() {
    clearView();
    const v = view();
    const sec = el('div', { class: 'section' },
      el('h2', null, 'Regional view'),
      el('div', { class: 'section-sub' },
        'Pick a geographic level, then a region, to see its peer geographies and top industries.'));
    const grid = el('div', { class: 'kpi-grid' });
    State.config.geographic_levels.forEach(L => {
      const dim = State.geographies[L.key] || [];
      const card = el('div', { class: 'kpi' },
        el('div', { class: 'kpi-label' }, L.display_name),
        el('div', { class: 'kpi-value' }, fmtNum(dim.length)),
        el('div', { class: 'kpi-detail' }, 'Click to browse'),
      );
      card.style.cursor = 'pointer';
      card.addEventListener('click', () => {
        document.getElementById('region-search').focus();
        renderLevelChooser(L.key);
      });
      grid.appendChild(card);
    });
    sec.appendChild(grid);

    const search = el('input', {
      id: 'region-search', placeholder: 'Search any region by name or geoid...',
      style: { width: '100%' }
    });
    sec.appendChild(el('div', { class: 'control', style: { marginTop: '14px' } },
      el('label', null, 'Search'), search));

    const results = el('div', { id: 'region-results', class: 'table-wrap',
      style: { marginTop: '12px' } });
    sec.appendChild(results);
    v.appendChild(sec);
    let searchIndex = null;
    let searchDebounce = null;

    function nameOf(level, row) {
      return row.county_name || row.state_name || row.cbsa_name ||
        row.csa_name || row.commuting_zone_name || row.name || '';
    }
    function idOf(level, row) {
      return row[geoidProperty(level)] || row.geoid;
    }

    function renderLevelChooser(level) {
      const dim = State.geographies[level] || [];
      const tbody = el('tbody');
      dim.slice(0, 200).forEach(row => {
        tbody.appendChild(makeClickableRow(
          () => navTo('#/regional/' + level + '/' + idOf(level, row)),
          { 'aria-label': 'Open ' + nameOf(level, row) + ' (' + idOf(level, row) + ')' },
          el('td', null, nameOf(level, row)),
          el('td', { class: 'num' }, idOf(level, row)),
          el('td', { class: 'num' }, fmtScore(row.economic_complexity_index))));
      });
      results.innerHTML = '';
      results.appendChild(el('table', { class: 'data' },
        el('thead', null, el('tr', null,
          el('th', null, 'Name'), el('th', null, 'ID'), el('th', null, 'ECI'))),
        tbody));
    }

    function buildSearchIndex() {
      if (searchIndex) return searchIndex;
      const idx = [];
      State.config.geographic_levels.forEach(L => {
        const dim = State.geographies[L.key] || [];
        dim.forEach(row => {
          const id = idOf(L.key, row);
          const name = nameOf(L.key, row);
          idx.push({
            level: L.key,
            level_name: L.display_name,
            id,
            name,
            eci: row.economic_complexity_index,
            _search: ((id || '') + ' ' + (name || '')).toLowerCase()
          });
        });
      });
      searchIndex = idx;
      return idx;
    }

    function doSearch() {
      const q = search.value.trim().toLowerCase();
      if (!q) { results.innerHTML = ''; return; }
      const rows = buildSearchIndex().filter(r => r._search.includes(q));
      const top = rows.slice(0, 200);
      const tbody = el('tbody');
      top.forEach(r => {
        tbody.appendChild(makeClickableRow(
          () => navTo('#/regional/' + r.level + '/' + r.id),
          { 'aria-label': 'Open ' + r.name + ' (' + r.id + ') at ' + r.level_name + ' level' },
          el('td', null, r.name),
          el('td', null, r.level_name),
          el('td', { class: 'num' }, r.id),
          el('td', { class: 'num' }, fmtScore(r.eci))));
      });
      results.innerHTML = '';
      results.appendChild(el('table', { class: 'data' },
        el('thead', null, el('tr', null,
          el('th', null, 'Name'), el('th', null, 'Level'),
          el('th', null, 'ID'), el('th', null, 'ECI'))),
        tbody));
    }
    search.addEventListener('input', () => {
      if (searchDebounce) clearTimeout(searchDebounce);
      searchDebounce = setTimeout(doSearch, 200);
    });
  }

  /* ---- regional detail (specific geography) ------------------------------- */
  async function renderRegional(level, id) {
    clearView();
    const v = view();

    const dim = State.geographies[level] || [];
    const idCol = geoidProperty(level);
    const row = dim.find(r => r[idCol] == id || r.geoid == id);
    if (!row) {
      v.appendChild(el('div', { class: 'section' },
        el('h2', null, 'Region not found'),
        el('div', { class: 'error-box' },
          'No ' + level + ' with ID ' + id + '.')));
      return;
    }

    const name = row.county_name || row.state_name || row.cbsa_name ||
      row.csa_name || row.commuting_zone_name || row.name || id;

    v.appendChild(el('div', { class: 'section' },
      el('h2', null, name),
      el('div', { class: 'section-sub' },
        levelMeta(level).display_name + ' · ID ' + id)));

    // KPI strip
    const kpis = el('div', { class: 'kpi-grid' },
      el('div', { class: 'kpi' },
        el('div', { class: 'kpi-label' }, 'Economic Complexity'),
        el('div', { class: 'kpi-value' }, fmtScore(row.economic_complexity_index)),
        el('div', { class: 'kpi-detail' },
          'Percentile ' + fmtScore(row.economic_complexity_percentile_score))),
      el('div', { class: 'kpi' },
        el('div', { class: 'kpi-label' }, 'Industrial Diversity'),
        el('div', { class: 'kpi-value' }, fmtNum(row.industrial_diversity)),
        el('div', { class: 'kpi-detail' }, 'Specializations (LQ ≥ 1)'))
    );
    if (row.n_constituent_counties != null) {
      kpis.appendChild(el('div', { class: 'kpi' },
        el('div', { class: 'kpi-label' }, 'Counties'),
        el('div', { class: 'kpi-value' }, fmtNum(row.n_constituent_counties)),
        el('div', { class: 'kpi-detail' },
          (row.n_constituent_states || '') + ' state(s)')));
    }
    v.appendChild(el('div', { class: 'section' }, kpis));

    // Two-up: top industries (left) + peer geographies (right)
    const grid = el('div', { class: 'row-3' });
    const indSec = el('div', { class: 'section' },
      el('h3', null, 'Top industries (by feasibility)'),
      el('div', { id: 'reg-industries' }, 'Loading...'));
    const peerSec = el('div', { class: 'section' },
      el('h3', null, 'Peer geographies (Jaccard top 25)'),
      el('div', { id: 'reg-peers' }, 'Loading...'));
    grid.appendChild(indSec);
    grid.appendChild(peerSec);
    v.appendChild(grid);

    // Lazy heavy sections
    const heavy = el('div', { class: 'section' },
      el('h3', null, 'Industry space + co-location'),
      el('div', { class: 'section-sub' },
        'These visualizations are large (industry-space network and ' +
        'industry co-location proximity). They load only when you ask.'),
      el('button', {
        class: 'collapsible-trigger',
        onclick: (e) => loadHeavy(e.target, level)
      }, 'Load industry space + co-location'),
      el('div', { class: 'collapsible-body', id: 'heavy-body' })
    );
    v.appendChild(heavy);

    // Industries table
    try {
      const slice = await PERF('reg:slice:' + level + ':' + id, () =>
        fetchCSV('data/by_geography/' + level + '/' + id + '.csv'));
      renderRegionalIndustries(slice, document.getElementById('reg-industries'), level, id);
    } catch (err) {
      document.getElementById('reg-industries').textContent =
        'Failed to load industry slice: ' + err.message;
    }

    // Peers (county/cbsa/csa/cz only)
    if (level !== 'state') {
      try {
        const peers = await PERF('reg:peers:' + level, () =>
          fetchCSVGZ('data/meta/peer_geography_' + level + '.csv.gz'));
        renderRegionalPeers(peers, document.getElementById('reg-peers'), level, id);
      } catch (err) {
        document.getElementById('reg-peers').textContent =
          'Failed to load peers: ' + err.message;
      }
    } else {
      document.getElementById('reg-peers').textContent =
        'Peer geography is not computed at the state level.';
    }
  }

  function renderRegionalIndustries(slice, container, level, id) {
    container.innerHTML = '';
    if (!slice || !slice.length) {
      container.textContent = 'No industry data for this region.';
      return;
    }
    // sort by feasibility desc, take top 50
    slice.forEach(r => {
      r.industry_feasibility = +r.industry_feasibility;
      r.industry_feasibility_percentile_score = +r.industry_feasibility_percentile_score;
      r.location_quotient = +r.location_quotient;
      r.industry_employment_share = +r.industry_employment_share;
    });
    const sorted = slice.slice().sort((a, b) =>
      (b.industry_feasibility || 0) - (a.industry_feasibility || 0)).slice(0, 50);
    const tbody = el('tbody');
    sorted.forEach(r => {
      const ind = State.industriesByCode[r.industry_code];
      const desc = ind ? ind.industry_description : r.industry_code;
      const isSpec = (r.location_quotient || 0) >= 1;
      const tr = el('tr', { class: isSpec ? 'spec' : '' },
        el('td', null, r.industry_code),
        el('td', null, desc),
        el('td', { class: 'num' }, fmtScore(r.industry_feasibility)),
        el('td', { class: 'num' }, fmtScore(r.industry_feasibility_percentile_score)),
        el('td', { class: 'num' }, fmtScore(r.location_quotient)),
        el('td', { class: 'num' }, fmtScore((r.industry_employment_share || 0) * 100)));
      tbody.appendChild(tr);
    });
    container.appendChild(el('div', { class: 'table-wrap' },
      el('table', { class: 'data' },
        el('thead', null, el('tr', null,
          el('th', null, 'NAICS-6'), el('th', null, 'Industry'),
          el('th', null, 'Feasibility'), el('th', null, '%ile'),
          el('th', null, 'LQ'), el('th', null, 'Share %'))),
        tbody)));
  }

  function renderRegionalPeers(peers, container, level, id) {
    container.innerHTML = '';
    const idCol = geoidProperty(level);
    const peerCol = 'peer_' + idCol;
    const own = peers.filter(p => (p[idCol] || '') == String(id));
    if (!own.length) {
      container.textContent = 'No peer entries for this region.';
      return;
    }
    own.sort((a, b) => (+a.peer_rank || 999) - (+b.peer_rank || 999));
    const top = own.slice(0, 25);
    const tbody = el('tbody');
    top.forEach(r => {
      const tr = el('tr', {
        onclick: () => navTo('#/regional/' + level + '/' + r[peerCol])
      },
        el('td', null, r.peer_rank),
        el('td', null, r.peer_name || r[peerCol]),
        el('td', { class: 'num' }, fmtScore(+r.jaccard_similarity)),
        el('td', { class: 'num' }, r.industries_in_common != null ? r.industries_in_common : '—'));
      tbody.appendChild(tr);
    });
    container.appendChild(el('div', { class: 'table-wrap' },
      el('table', { class: 'data' },
        el('thead', null, el('tr', null,
          el('th', null, '#'), el('th', null, 'Peer'),
          el('th', null, 'Jaccard'), el('th', null, 'Industries in common'))),
        tbody)));
  }

  async function loadHeavy(btn, level) {
    btn.disabled = true;
    btn.textContent = 'Loading...';
    const body = document.getElementById('heavy-body');
    body.classList.add('open');
    try {
      // Industry space
      if (levelMeta(level).supports_industry_space) {
        const [nodes, edges] = await Promise.all([
          fetchCSVGZ('data/meta/industry_space_nodes_' + level + '.csv.gz'),
          fetchCSVGZ('data/meta/industry_space_edges_' + level + '.csv.gz'),
        ]);
        const netDiv = el('div', null,
          el('h3', null, 'Industry space network'),
          el('div', { class: 'section-sub' },
            nodes.length + ' industries · ' + edges.length + ' top edges'),
          el('svg', { class: 'network', id: 'heavy-network' }));
        body.appendChild(netDiv);
        renderIndustryNetwork(nodes, edges);
      } else {
        body.appendChild(el('div', { class: 'collapsible-note' },
          'Industry space is not computed for the state level.'));
      }

      // Co-location proximity heatmap (top 30 industries)
      if (level !== 'state') {
        const colo = await fetchCSVGZ('data/meta/colocation/' + level + '.csv.gz');
        body.appendChild(el('h3', null, 'Industry co-location (top peers)'));
        const heatDiv = el('div', { class: 'heatmap', id: 'heavy-heatmap' });
        body.appendChild(heatDiv);
        renderColocationHeatmap(colo, heatDiv);
      }

      btn.textContent = 'Loaded';
    } catch (err) {
      body.appendChild(el('div', { class: 'error-box' },
        'Failed to load heavy section: ' + err.message));
      btn.disabled = false;
      btn.textContent = 'Retry';
    }
  }

  function renderIndustryNetwork(nodes, edges) {
    const svgEl = document.getElementById('heavy-network');
    const svg = d3.select(svgEl);
    svg.selectAll('*').remove();
    const w = svgEl.clientWidth, h = svgEl.clientHeight;

    nodes.forEach(n => {
      n.layout_x = +n.layout_x;
      n.layout_y = +n.layout_y;
      n.industry_complexity = +n.industry_complexity;
      n.industry_centrality = +n.industry_centrality;
    });
    const xExt = d3.extent(nodes, n => n.layout_x);
    const yExt = d3.extent(nodes, n => n.layout_y);
    const xs = d3.scaleLinear().domain(xExt).range([20, w - 20]);
    const ys = d3.scaleLinear().domain(yExt).range([20, h - 20]);

    // Subsample edges for performance — top 800 by weight
    edges.forEach(e => { e.weight = +e.weight; });
    const eSorted = edges.slice().sort((a, b) => b.weight - a.weight).slice(0, 800);

    const nByCode = {};
    nodes.forEach(n => { nByCode[n.industry_code] = n; });

    svg.append('g').selectAll('line').data(eSorted).enter().append('line')
      .attr('x1', d => xs(nByCode[d.from] ? nByCode[d.from].layout_x : 0))
      .attr('y1', d => ys(nByCode[d.from] ? nByCode[d.from].layout_y : 0))
      .attr('x2', d => xs(nByCode[d.to] ? nByCode[d.to].layout_x : 0))
      .attr('y2', d => ys(nByCode[d.to] ? nByCode[d.to].layout_y : 0))
      .attr('stroke-width', d => 0.5 + 1.5 * d.weight);

    const ext = d3.extent(nodes, n => n.industry_complexity);
    const colorScale = d3.scaleSequential(d3.interpolateViridis).domain(ext);

    svg.append('g').selectAll('circle').data(nodes).enter().append('circle')
      .attr('cx', d => xs(d.layout_x)).attr('cy', d => ys(d.layout_y))
      .attr('r', d => 3 + 12 * d.industry_centrality)
      .attr('fill', d => colorScale(d.industry_complexity))
      .on('mousemove', (e, d) => tt(
        '<strong>' + d.industry_code + '</strong> ' +
        (d.industry_description || '') + '<br>' +
        'Complexity: ' + fmtScore(d.industry_complexity) + '<br>' +
        'Centrality: ' + fmtScore(d.industry_centrality), e))
      .on('mouseleave', ttHide);
  }

  function renderColocationHeatmap(colo, container) {
    if (!colo || !colo.length) {
      container.textContent = 'No co-location data.';
      return;
    }
    // For each industry, take top 10 peers; visualize a 30x10 grid of top
    // industries (by row count) as a heatmap.
    colo.forEach(r => { r.proximity = +r.proximity; r.proximity_rank = +r.proximity_rank; });
    const top10 = colo.filter(r => r.proximity_rank <= 10);
    const rowCounts = {};
    top10.forEach(r => { rowCounts[r.industry_code] = (rowCounts[r.industry_code] || 0) + 1; });
    const topIndustries = Object.keys(rowCounts).slice(0, 30);
    const filtered = top10.filter(r => topIndustries.indexOf(r.industry_code) >= 0);

    const cellW = 28, cellH = 22, labelW = 220;
    const w = labelW + 10 * cellW + 10;
    const h = topIndustries.length * cellH + 30;
    const svg = d3.select(container).append('svg')
      .attr('width', w).attr('height', h);

    const ext = d3.extent(filtered, r => r.proximity);
    const cs = d3.scaleSequential(d3.interpolateBlues).domain(ext);

    topIndustries.forEach((ic, rowIdx) => {
      const ind = State.industriesByCode[ic];
      svg.append('text')
        .attr('x', labelW - 6).attr('y', 24 + rowIdx * cellH + cellH / 2 + 4)
        .attr('text-anchor', 'end')
        .text((ind ? ind.industry_description : ic).slice(0, 32));

      const rows = filtered.filter(r => r.industry_code === ic)
        .sort((a, b) => a.proximity_rank - b.proximity_rank);
      rows.forEach((r, colIdx) => {
        svg.append('rect')
          .attr('x', labelW + colIdx * cellW)
          .attr('y', 24 + rowIdx * cellH)
          .attr('width', cellW - 1).attr('height', cellH - 1)
          .attr('fill', cs(r.proximity))
          .on('mousemove', (e) => tt(
            '<strong>' + r.industry_code + ' ⇄ ' + r.peer_industry_code + '</strong><br>' +
            (r.peer_industry_description || '') + '<br>' +
            'Rank ' + r.proximity_rank + ' · proximity ' + fmtScore(r.proximity), e))
          .on('mouseleave', ttHide);
      });
    });
  }

  /* ---- start ------------------------------------------------------------- */
  document.addEventListener('DOMContentLoaded', boot);
})();
