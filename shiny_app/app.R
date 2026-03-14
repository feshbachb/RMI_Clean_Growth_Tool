# ══════════════════════════════════════════════════════════════════════════════
#  RMI Clean Growth Tool — R Shiny Dashboard (shinylive-compatible)
# ══════════════════════════════════════════════════════════════════════════════
#
# Runs entirely in the browser via WebAssembly (shinylive) on GitHub Pages.
# Fetches data from the existing static GitHub Pages site — no server needed.
#
# Packages (all available in webR/shinylive):
#   shiny, bslib, plotly, DT, visNetwork, dplyr, jsonlite
#
# Local dev:
#   shiny::runApp("shiny_app")
#
# Deploy to GitHub Pages:
#   shinylive::export("shiny_app", "docs/shiny")
#   # Then push docs/ — GitHub Actions handles this automatically
# ══════════════════════════════════════════════════════════════════════════════

library(shiny)
library(bslib)
library(plotly)
library(DT)
library(visNetwork)
library(dplyr)
library(jsonlite)

# ── Data URL configuration ────────────────────────────────────────────────────
# Data is served from the existing GitHub Pages static site.
# When running locally, override with: options(rmi_base_url = "http://localhost:8080/")
BASE_URL <- getOption("rmi_base_url",
  "https://feshbachb.github.io/RMI_Clean_Growth_Tool/"
)

# ── Data fetching (works in both regular R and webR/shinylive) ────────────────
fetch_csv <- function(rel_path) {
  url <- paste0(BASE_URL, rel_path)
  tmp <- tempfile(fileext = ".csv")
  tryCatch({
    download.file(url, tmp, quiet = TRUE)
    read.csv(tmp, stringsAsFactors = FALSE)
  }, error = function(e) {
    warning("Failed to fetch: ", url, " \u2014 ", e$message)
    showNotification(paste("Failed to load:", rel_path), type = "error",
                     duration = 8)
    data.frame()
  }, finally = unlink(tmp))
}

fetch_csv_gz <- function(rel_path) {
  url <- paste0(BASE_URL, rel_path)
  tmp <- tempfile(fileext = ".csv.gz")
  tryCatch({
    download.file(url, tmp, mode = "wb", quiet = TRUE)
    read.csv(gzfile(tmp), stringsAsFactors = FALSE)
  }, error = function(e) {
    warning("Failed to fetch: ", url, " \u2014 ", e$message)
    showNotification(paste("Failed to load:", rel_path), type = "error",
                     duration = 8)
    data.frame()
  }, finally = unlink(tmp))
}

# URL to county GeoJSON (plotly.js fetches this directly in the browser)
# Note: plotly choropleth requires GeoJSON (not TopoJSON), so we use the full file here.
# The main HTML dashboard uses the smaller TopoJSON + topojson-client for D3 rendering.
COUNTY_GEOJSON_URL <- paste0(BASE_URL, "us-counties-2023.json")

# Filter territories: only keep continental US + DC (state FIPS <= 56)
filter_continental <- function(df, geoid_col = "geoid") {
  state_fips <- as.integer(substr(as.character(df[[geoid_col]]), 1, 2))
  df[!is.na(state_fips) & state_fips <= 56, ]
}

# ── Helpers ───────────────────────────────────────────────────────────────────
pad_geoid <- function(geoid, level) {
  geoid <- as.character(geoid)
  if (level == "county") return(formatC(as.integer(geoid), width = 5, flag = "0"))
  if (level == "state")  return(formatC(as.integer(geoid), width = 2, flag = "0"))
  geoid
}

# Quantile color scale: transform values to rank-based [0,1] for uniform color
quantile_transform <- function(vals) {
  out <- rep(NA_real_, length(vals))
  valid <- !is.na(vals)
  if (sum(valid) == 0) return(out)
  out[valid] <- rank(vals[valid], ties.method = "average") / sum(valid)
  out
}

# YlGnBu colorscale for plotly (matches the HTML dashboard)
YLGNBU_SCALE <- list(
  list(0,    "#ffffd9"),
  list(0.125, "#edf8b1"),
  list(0.25,  "#c7e9b4"),
  list(0.375, "#7fcdbb"),
  list(0.5,   "#41b6c4"),
  list(0.625, "#1d91c0"),
  list(0.75,  "#225ea8"),
  list(0.875, "#253494"),
  list(1,     "#081d58")
)

BINARY_SCALE <- list(list(0, "#e8e8e8"), list(1, "#2c7bb6"))

BINARY_METRICS <- c("industry_present", "industry_comparative_advantage",
                     "strategic_gain_possible")

# ── Level mapping ─────────────────────────────────────────────────────────────
LEVEL_CODES  <- c(county = 1, state = 2, cbsa = 3, csa = 4, cz = 5)
LEVEL_LABELS <- c(county = "County", state = "State",
                   cbsa = "Metropolitan Statistical Area",
                   csa = "Combined Statistical Area",
                   cz = "Commuting Zone")

GEO_METRICS <- c(
  "Economic Complexity Index"        = "economic_complexity_index",
  "Economic Complexity Percentile"   = "economic_complexity_percentile_score",
  "Industrial Diversity"             = "industrial_diversity",
  "Strategic Index"                  = "strategic_index",
  "Strategic Index Percentile"       = "strategic_index_percentile"
)

IND_METRICS <- c(
  "Industry Feasibility"             = "industry_feasibility",
  "Industry Feasibility Percentile"  = "industry_feasibility_percentile_score",
  "Industry Presence"                = "industry_present",
  "Location Quotient"                = "location_quotient",
  "Comparative Advantage"            = "industry_comparative_advantage",
  "Strategic Gain"                   = "strategic_gain",
  "Strategic Gain Percentile"        = "strategic_gain_percentile_score",
  "Strategic Gain Possible"          = "strategic_gain_possible",
  "Employment Share"                 = "industry_employment_share"
)

# ── RMI brand ─────────────────────────────────────────────────────────────────
RMI_BLUE   <- "#1B3A4B"
RMI_ENERGY <- "#45CFCC"

# ══════════════════════════════════════════════════════════════════════════════
#  UI
# ══════════════════════════════════════════════════════════════════════════════
APP_CSS <- "
/* ── Loading overlay (visible until metadata loads) ── */
#loading-overlay {
  position: fixed; top: 0; left: 0; right: 0; bottom: 0;
  background: #fff; z-index: 9999;
  display: flex; align-items: center; justify-content: center;
  flex-direction: column; gap: 16px;
}
#loading-overlay.loaded {
  opacity: 0; pointer-events: none;
  transition: opacity 0.4s ease;
}
#loading-overlay .loading-text {
  color: #1B3A4B; font-size: 16px; font-weight: 500;
}

/* ── Spinner animation ── */
@keyframes rmi-spin {
  to { transform: rotate(360deg); }
}
.rmi-spinner {
  width: 44px; height: 44px;
  border: 4px solid #e0e0e0;
  border-top-color: #45CFCC;
  border-radius: 50%;
  animation: rmi-spin 0.8s linear infinite;
}
.rmi-spinner-sm {
  width: 28px; height: 28px; border-width: 3px;
}

/* ── Busy indicator (top bar) ── */
.shiny-busy .busy-bar {
  display: block !important;
}
.busy-bar {
  display: none;
  position: fixed; top: 0; left: 0; right: 0;
  height: 3px; z-index: 2000;
  background: linear-gradient(90deg, #45CFCC 0%, #1B3A4B 50%, #45CFCC 100%);
  background-size: 200% 100%;
  animation: busy-slide 1.5s linear infinite;
}
@keyframes busy-slide {
  0% { background-position: 200% 0; }
  100% { background-position: -200% 0; }
}

/* ── Loading placeholder for content areas ── */
.loading-placeholder {
  display: flex; flex-direction: column;
  align-items: center; justify-content: center;
  padding: 48px 16px; color: #888; gap: 12px;
}
.loading-placeholder .rmi-spinner { margin-bottom: 4px; }

/* ── Button loading state ── */
.btn-loading { pointer-events: none; opacity: 0.7; }
.btn-loading::after {
  content: ''; display: inline-block;
  width: 14px; height: 14px; margin-left: 8px;
  border: 2px solid #fff; border-top-color: transparent;
  border-radius: 50%;
  animation: rmi-spin 0.7s linear infinite;
  vertical-align: middle;
}

/* ── Mobile: iPhone & small screens ── */
@media (max-width: 767px) {
  /* Let content scroll naturally — don't squish into viewport */
  .bslib-page-fill, .bslib-gap-spacing,
  .tab-content, .tab-pane, .tab-pane > .bslib-sidebar-layout {
    height: auto !important;
    min-height: 0 !important;
    max-height: none !important;
  }

  /* Sidebar: collapsible overlay */
  .bslib-sidebar-layout {
    display: flex !important;
    flex-direction: column !important;
    height: auto !important;
  }
  .bslib-sidebar-layout > .sidebar {
    width: 100% !important;
    max-width: 100% !important;
    position: relative !important;
  }
  .bslib-sidebar-layout > .main {
    width: 100% !important;
    max-width: 100% !important;
    flex: none !important;
  }

  /* Navbar: compact & wrap */
  .navbar { flex-wrap: wrap; }
  .navbar-brand { font-size: 14px !important; }
  .navbar-brand img { height: 20px !important; margin-right: 6px !important; }
  .navbar-nav { flex-wrap: wrap; }
  .nav-link {
    font-size: 12px !important;
    padding: 6px 8px !important;
    white-space: nowrap;
  }

  /* Prevent iOS input zoom (font-size must be >= 16px) */
  .selectize-input, .selectize-input input,
  .form-control, .form-select,
  select, input[type='text'] { font-size: 16px !important; }

  /* Cards: responsive wrap */
  .d-flex.flex-wrap { gap: 6px !important; }
  .card { min-width: 0 !important; flex: 1 1 45% !important; }
  .card .card-body { padding: 8px !important; }
  .card .card-body h5 { font-size: 16px !important; }
  .card .card-body .text-muted { font-size: 11px !important; }

  /* Map & chart: proportional to screen */
  .plotly .modebar { display: none !important; }
  .js-plotly-plot { height: 300px !important; }
  .visNetwork { height: 350px !important; }

  /* Two-column rows: stack vertically */
  .row > .col-sm-6, .row > [class*='col-'] {
    width: 100% !important; flex: 0 0 100% !important;
    max-width: 100% !important;
  }

  /* Tables: horizontal scroll */
  .dataTables_wrapper {
    overflow-x: auto; -webkit-overflow-scrolling: touch;
    font-size: 13px;
  }

  /* Spacing */
  .container-fluid { padding: 8px !important; }
  h3 { font-size: 18px !important; }
  h5 { font-size: 14px !important; }
}

/* ── Touch devices: bigger tap targets ── */
@media (hover: none) and (pointer: coarse) {
  .selectize-input { min-height: 44px !important; }
  .selectize-input input { min-height: 36px !important; }
  .btn { min-height: 44px !important; font-size: 15px !important; }
  .form-select, .form-control { min-height: 44px !important; }
  .selectize-dropdown-content .option { padding: 12px !important; }
  .nav-link { min-height: 44px !important; display: flex; align-items: center; }
}
"

# JavaScript for loading overlay + button busy states
APP_JS <- "
$(document).on('shiny:connected', function() {
  // Hide overlay once first output renders (metadata loaded)
  $(document).one('shiny:value', function() {
    $('#loading-overlay').addClass('loaded');
    setTimeout(function() { $('#loading-overlay').remove(); }, 500);
  });
});

// Button loading states during computation
$(document).on('shiny:busy', function() {
  $('.btn-primary').addClass('btn-loading');
});
$(document).on('shiny:idle', function() {
  $('.btn-primary').removeClass('btn-loading');
});
"

ui <- page_navbar(
  title = tags$span(
    tags$img(
      src = "https://rmi.org/wp-content/uploads/2021/06/rmi-logo-white.svg",
      height = "28px",
      style = "margin-right:12px; vertical-align:middle;"
    ),
    "Clean Growth Tool"
  ),
  theme = bs_theme(
    version = 5, bg = "#fff", fg = RMI_BLUE, primary = RMI_ENERGY,
    "navbar-bg" = RMI_BLUE,
    base_font = font_google("Source Sans Pro")
  ),
  fillable = FALSE,
  header = tagList(
    tags$head(
      tags$meta(name = "viewport",
                content = "width=device-width, initial-scale=1.0, viewport-fit=cover"),
      tags$style(HTML(APP_CSS)),
      tags$script(HTML(APP_JS))
    ),
    # Loading overlay — visible until metadata loads
    tags$div(id = "loading-overlay",
      tags$div(class = "rmi-spinner"),
      tags$div(class = "loading-text", "Loading Clean Growth Tool...")
    ),
    # Top busy bar — visible during any Shiny computation
    tags$div(class = "busy-bar")
  ),

  # ── Tab 1: Regional View ────────────────────────────────────────────────
  nav_panel("Regional",
    layout_sidebar(
      sidebar = sidebar(width = 300,
        selectInput("reg_level", "Geography Level",
                    setNames(names(LEVEL_LABELS), LEVEL_LABELS), "county"),
        selectizeInput("reg_geo", "Search Geography", choices = NULL,
                       options = list(placeholder = "Type to search...")),
        actionButton("reg_load", "Load Data", class = "btn-primary w-100 mt-2")
      ),
      uiOutput("reg_ui")
    )
  ),

  # ── Tab 2: National View ────────────────────────────────────────────────
  nav_panel("National",
    layout_sidebar(
      sidebar = sidebar(width = 300,
        selectInput("nat_level", "Geography Level",
                    setNames(names(LEVEL_LABELS), LEVEL_LABELS), "county"),
        conditionalPanel("input.nat_level == 'county'",
          selectInput("nat_state", "Filter by State", choices = c("All States" = ""))
        ),
        radioButtons("nat_mode", "Mode",
                     c("Industry Metrics" = "industry",
                       "Geography Metrics" = "geography"),
                     "industry", inline = TRUE),
        conditionalPanel("input.nat_mode == 'industry'",
          selectizeInput("nat_industry", "Industry", choices = NULL,
                         options = list(placeholder = "Search industry...")),
          selectInput("nat_metric", "Metric", IND_METRICS, "industry_feasibility")
        ),
        conditionalPanel("input.nat_mode == 'geography'",
          selectInput("nat_geo_metric", "Metric", GEO_METRICS,
                      "economic_complexity_index")
        ),
        actionButton("nat_load", "Load Data", class = "btn-primary w-100 mt-2")
      ),
      uiOutput("nat_ui")
    )
  ),

  # ── Tab 3: Industry Space ───────────────────────────────────────────────
  nav_panel("Industry Space",
    layout_sidebar(
      sidebar = sidebar(width = 300,
        selectInput("is_level", "Geography Level",
                    setNames(names(LEVEL_LABELS), LEVEL_LABELS), "county"),
        selectizeInput("is_geo", "Highlight Geography", choices = NULL,
                       options = list(placeholder = "Search geography...")),
        sliderInput("is_thresh", "Edge Weight Threshold (percentile)",
                    0, 100, 75, 5),
        actionButton("is_load", "Load Network", class = "btn-primary w-100 mt-2")
      ),
      uiOutput("is_ui")
    )
  ),

  nav_spacer(),
  nav_item(tags$a("RMI", href = "https://rmi.org", target = "_blank",
                   style = paste0("color:", RMI_ENERGY, ";")))
)

# ══════════════════════════════════════════════════════════════════════════════
#  SERVER
# ══════════════════════════════════════════════════════════════════════════════
server <- function(input, output, session) {

  # ── Load metadata at startup ────────────────────────────────────────────
  meta <- reactiveValues(
    geo = NULL, ind_titles = NULL, crosswalk = NULL,
    ind_meta = NULL, is_nodes = NULL, is_edges = NULL,
    loaded = FALSE
  )

  observe({
    if (!meta$loaded) {
      withProgress(message = "Loading metadata...", {
        incProgress(0.1)
        meta$geo        <- fetch_csv("meta/geography_specific.csv")
        incProgress(0.3)
        meta$ind_titles <- fetch_csv("meta/industry_titles.csv")
        incProgress(0.1)
        meta$crosswalk  <- fetch_csv("meta/crosswalk.csv")
        if (nrow(meta$crosswalk) > 0) {
          meta$crosswalk$state_fips <- formatC(
            as.integer(meta$crosswalk$state_fips), width = 2, flag = "0")
          meta$crosswalk$county_geoid <- formatC(
            as.integer(meta$crosswalk$county_geoid), width = 5, flag = "0")
        }
        incProgress(0.2)
        meta$ind_meta   <- fetch_csv("meta/industry_specific.csv")
        incProgress(0.2)
        meta$loaded <- TRUE
      })

      # Populate industry dropdown
      if (nrow(meta$ind_titles) > 0) {
        choices <- setNames(
          as.character(meta$ind_titles$industry_code),
          paste(meta$ind_titles$industry_code, "-",
                meta$ind_titles$industry_description)
        )
        updateSelectizeInput(session, "nat_industry",
                             choices = choices, selected = "335910",
                             server = TRUE)
      }

      # Populate state filter
      if (nrow(meta$crosswalk) > 0) {
        states <- meta$crosswalk %>%
          distinct(state_fips, state_name) %>%
          arrange(state_name)
        updateSelectInput(session, "nat_state",
                          choices = c("All States" = "",
                                      setNames(states$state_fips, states$state_name)))
      }
    }
  })

  # ── Geography dropdown helpers ──────────────────────────────────────────
  geo_choices <- function(level) {
    req(meta$geo)
    code <- LEVEL_CODES[[level]]
    geos <- meta$geo %>% filter(geo_aggregation_level == code)
    if (level == "county" && nrow(meta$crosswalk) > 0) {
      geos$geoid_pad <- formatC(as.integer(geos$geoid), width = 5, flag = "0")
      geos <- geos %>%
        left_join(meta$crosswalk %>%
                    distinct(county_geoid, state_abbreviation),
                  by = c("geoid_pad" = "county_geoid")) %>%
        mutate(label = paste0(name, ", ", state_abbreviation))
    } else {
      geos$label <- geos$name
    }
    setNames(as.character(geos$geoid), geos$label)
  }

  observe({
    updateSelectizeInput(session, "reg_geo",
                         choices = geo_choices(input$reg_level), server = TRUE)
  })
  observe({
    updateSelectizeInput(session, "is_geo",
                         choices = geo_choices(input$is_level), server = TRUE)
  })

  # ── Helper: display name for a geography ────────────────────────────────
  geo_display_name <- function(geoid, level) {
    req(meta$geo)
    code <- LEVEL_CODES[[level]]
    rec <- meta$geo %>%
      filter(geo_aggregation_level == code,
             as.character(.data$geoid) == as.character(!!geoid))
    if (nrow(rec) == 0) return(as.character(geoid))
    nm <- rec$name[1]
    if (level == "county" && nrow(meta$crosswalk) > 0) {
      pad <- formatC(as.integer(geoid), width = 5, flag = "0")
      st <- meta$crosswalk %>%
        filter(county_geoid == pad) %>%
        slice(1) %>%
        pull(state_abbreviation)
      if (length(st) > 0 && !is.na(st)) nm <- paste0(nm, ", ", st)
    }
    nm
  }

  # ═══════════════════════════════════════════════════════════════════════════
  #  TAB 1: REGIONAL VIEW
  # ═══════════════════════════════════════════════════════════════════════════
  reg_data <- eventReactive(input$reg_load, {
    req(input$reg_geo, meta$loaded)
    level <- input$reg_level
    geoid <- input$reg_geo
    padded <- pad_geoid(geoid, level)

    withProgress(message = "Loading regional data...", {
      data <- fetch_csv_gz(paste0("by_geography/", level, "/", padded, ".csv.gz"))
    })
    if (nrow(data) == 0) return(NULL)

    code <- LEVEL_CODES[[level]]
    geo_rec <- meta$geo %>%
      filter(geo_aggregation_level == code,
             as.character(.data$geoid) == as.character(!!geoid))

    # Enrich with industry names
    ind_lu <- setNames(meta$ind_titles$industry_description,
                        as.character(meta$ind_titles$industry_code))
    data$industry_description <- ind_lu[as.character(data$industry_code)]

    # Join industry complexity
    im <- meta$ind_meta %>%
      filter(geo_aggregation_level == code) %>%
      select(industry_code, industry_complexity, industry_complexity_percentile)
    data <- data %>%
      left_join(im, by = "industry_code")

    list(data = data, geo_rec = geo_rec, level = level, geoid = padded)
  })

  output$reg_ui <- renderUI({
    rd <- reg_data()
    if (is.null(rd)) {
      return(tags$div(class = "loading-placeholder",
        tags$h4("Select a Geography",
                style = paste0("color:", RMI_BLUE, ";")),
        tags$p(class = "text-muted",
               "Choose a geography level and search for a region, then click",
               tags$strong("Load Data."))
      ))
    }

    geo_rec <- rd$geo_rec
    nm <- if (nrow(geo_rec) > 0) geo_display_name(rd$geo_rec$geoid[1], rd$level)
          else rd$geoid

    # Summary cards
    cards_ui <- NULL
    if (nrow(geo_rec) > 0) {
      r <- geo_rec[1, ]
      make_card <- function(lbl, val, sub = "") {
        tags$div(class = "card text-center m-1",
                 style = "min-width:100px; flex:1 1 auto;",
          tags$div(class = "card-body py-2",
            tags$small(class = "text-muted", lbl),
            tags$h5(val, style = paste0("color:", RMI_BLUE, ";")),
            if (nzchar(sub)) tags$small(class = "text-muted", sub)
          ))
      }
      cards_ui <- tags$div(class = "d-flex flex-wrap justify-content-center my-3",
        make_card("Economic Complexity", sprintf("%.2f", r$economic_complexity_index),
                  paste0(sprintf("%.1f", r$economic_complexity_percentile_score), "% pctl")),
        make_card("Industrial Diversity",
                  format(round(r$industrial_diversity), big.mark = ",")),
        make_card("Strategic Index", sprintf("%.2f", r$strategic_index),
                  paste0(sprintf("%.1f", r$strategic_index_percentile), "% pctl"))
      )
    }

    tagList(
      tags$h3(nm, style = paste0("color:", RMI_BLUE, ";")),
      cards_ui,
      tags$h5("Current Strengths (Comparative Advantage)",
              class = "mt-4", style = paste0("color:", RMI_BLUE, ";")),
      DTOutput("reg_strengths"),
      tags$h5("Growth Opportunities (Feasibility \u2265 75th Pctl)",
              class = "mt-4", style = paste0("color:", RMI_BLUE, ";")),
      DTOutput("reg_growth"),
      tags$h5("All Industries",
              class = "mt-4", style = paste0("color:", RMI_BLUE, ";")),
      DTOutput("reg_all")
    )
  })

  reg_dt <- function(df, cols) {
    datatable(df %>% select(all_of(cols)),
              options = list(pageLength = 15, scrollX = TRUE),
              rownames = FALSE) %>%
      formatRound(intersect(cols,
        c("location_quotient", "industry_feasibility", "strategic_gain",
          "industry_complexity", "industry_employment_share")), 3)
  }

  output$reg_strengths <- renderDT({
    rd <- reg_data(); req(rd)
    df <- rd$data %>%
      filter(industry_comparative_advantage == 1) %>%
      arrange(desc(location_quotient))
    reg_dt(df, c("industry_description", "industry_code", "location_quotient",
                 "industry_employment_share", "industry_complexity",
                 "industry_feasibility_percentile_score"))
  })

  output$reg_growth <- renderDT({
    rd <- reg_data(); req(rd)
    df <- rd$data %>%
      filter(industry_comparative_advantage == 0,
             industry_feasibility_percentile_score >= 75) %>%
      arrange(desc(industry_feasibility))
    reg_dt(df, c("industry_description", "industry_code",
                 "industry_feasibility", "industry_feasibility_percentile_score",
                 "strategic_gain", "industry_complexity"))
  })

  output$reg_all <- renderDT({
    rd <- reg_data(); req(rd)
    reg_dt(rd$data %>% arrange(desc(industry_feasibility)),
           c("industry_description", "industry_code",
             "industry_present", "industry_comparative_advantage",
             "location_quotient", "industry_feasibility", "strategic_gain"))
  })

  # ═══════════════════════════════════════════════════════════════════════════
  #  TAB 2: NATIONAL VIEW
  # ═══════════════════════════════════════════════════════════════════════════
  nat_data <- eventReactive(input$nat_load, {
    req(meta$loaded)
    level <- input$nat_level
    mode  <- input$nat_mode

    if (mode == "industry") {
      ind_code <- input$nat_industry
      req(ind_code)
      metric_key   <- input$nat_metric
      metric_label <- names(IND_METRICS)[IND_METRICS == metric_key]

      withProgress(message = "Loading industry data...", {
        data <- fetch_csv_gz(
          paste0("by_industry/", level, "/", ind_code, ".csv.gz"))
      })
      if (nrow(data) == 0) return(NULL)

      # Pad geoids and join names
      data$geoid <- pad_geoid(data$geoid, level)
      data <- join_geo_names(data, level, meta)

      # State filter
      if (level == "county" && nzchar(input$nat_state)) {
        data <- data %>% filter(startsWith(geoid, input$nat_state))
      }

      ind_desc <- meta$ind_titles$industry_description[
        meta$ind_titles$industry_code == as.integer(ind_code)]
      title <- paste0(ind_code, " - ", ind_desc)

      list(data = data, metric_key = metric_key, metric_label = metric_label,
           level = level, mode = mode, title = title)

    } else {
      metric_key   <- input$nat_geo_metric
      metric_label <- names(GEO_METRICS)[GEO_METRICS == metric_key]
      code <- LEVEL_CODES[[level]]

      data <- meta$geo %>%
        filter(geo_aggregation_level == code) %>%
        mutate(geoid = pad_geoid(geoid, level))

      data <- join_geo_names(data, level, meta)

      if (level == "county" && nzchar(input$nat_state)) {
        data <- data %>% filter(startsWith(geoid, input$nat_state))
      }

      list(data = data, metric_key = metric_key, metric_label = metric_label,
           level = level, mode = mode, title = metric_label)
    }
  })

  output$nat_ui <- renderUI({
    nd <- nat_data()
    if (is.null(nd)) {
      return(tags$div(class = "loading-placeholder",
        tags$h4("National Comparison",
                style = paste0("color:", RMI_BLUE, ";")),
        tags$p(class = "text-muted",
               "Select filters and click", tags$strong("Load Data."))
      ))
    }
    tagList(
      tags$h3(nd$title, style = paste0("color:", RMI_BLUE, ";")),
      plotlyOutput("nat_map", height = "550px"),
      uiOutput("nat_cards"),
      fluidRow(
        column(6, DTOutput("nat_table")),
        column(6, plotlyOutput("nat_hist", height = "300px"))
      )
    )
  })

  output$nat_map <- renderPlotly({
    nd <- nat_data(); req(nd)
    level      <- nd$level
    metric_key <- nd$metric_key
    data       <- nd$data

    # Filter territories (keep continental US + DC only)
    if (level %in% c("county", "state")) {
      data <- filter_continental(data)
    }

    vals       <- data[[metric_key]]
    is_binary <- metric_key %in% BINARY_METRICS

    if (level == "state") {
      # Use plotly's built-in US state map (no GeoJSON needed)
      # Need state abbreviations
      if (!"state_abbreviation" %in% names(data) && nrow(meta$crosswalk) > 0) {
        st_map <- meta$crosswalk %>%
          distinct(state_fips, state_abbreviation)
        data <- data %>%
          left_join(st_map, by = c("geoid" = "state_fips"))
      }
      if (is_binary) {
        p <- plot_ly() %>%
          add_trace(
            type = "choropleth", locationmode = "USA-states",
            locations = data$state_abbreviation, z = vals,
            colorscale = BINARY_SCALE, zmin = 0, zmax = 1,
            text = data$display_name,
            hovertemplate = "<b>%{text}</b><br>%{z}<extra></extra>"
          )
      } else {
        z_qt <- quantile_transform(vals)
        # Compute percentile rank for hover
        pct_rank <- rank(vals, ties.method = "average", na.last = "keep")
        pct_rank <- round(100 * pct_rank / sum(!is.na(vals)))
        p <- plot_ly() %>%
          add_trace(
            type = "choropleth", locationmode = "USA-states",
            locations = data$state_abbreviation,
            z = z_qt,
            customdata = matrix(c(vals, pct_rank), ncol = 2),
            colorscale = YLGNBU_SCALE,
            text = data$display_name,
            hovertemplate = paste0(
              "<b>%{text}</b><br>",
              nd$metric_label, ": %{customdata[0]:.3f}<br>",
              "Percentile: %{customdata[1]:.0f}%",
              "<extra></extra>"
            ),
            colorbar = list(
              title = nd$metric_label,
              tickvals = seq(0, 1, length.out = 5),
              ticktext = sprintf("%.2f",
                quantile(vals, seq(0, 1, length.out = 5), na.rm = TRUE))
            )
          )
      }
      p %>% layout(geo = list(scope = "usa",
                               projection = list(type = "albers usa"),
                               showlakes = FALSE))

    } else {
      # County-level (or CBSA/CSA/CZ painted on counties)
      plot_data <- data
      if (!level %in% c("county", "state")) {
        # Map parent region values onto counties via crosswalk
        plot_data <- expand_to_counties(data, level, metric_key, meta)
      }

      if (is_binary) {
        p <- plot_ly() %>%
          add_trace(
            type = "choropleth",
            geojson = COUNTY_GEOJSON_URL,
            featureidkey = "id",
            locations = plot_data$geoid,
            z = plot_data[[metric_key]],
            colorscale = BINARY_SCALE, zmin = 0, zmax = 1,
            text = plot_data$display_name,
            hovertemplate = "<b>%{text}</b><br>%{z}<extra></extra>",
            marker = list(line = list(width = 0.3, color = "#ffffff"))
          )
      } else {
        z_qt <- quantile_transform(plot_data[[metric_key]])
        raw_vals <- plot_data[[metric_key]]
        pct_rank <- rank(raw_vals, ties.method = "average", na.last = "keep")
        pct_rank <- round(100 * pct_rank / sum(!is.na(raw_vals)))
        p <- plot_ly() %>%
          add_trace(
            type = "choropleth",
            geojson = COUNTY_GEOJSON_URL,
            featureidkey = "id",
            locations = plot_data$geoid,
            z = z_qt,
            customdata = matrix(c(raw_vals, pct_rank), ncol = 2),
            colorscale = YLGNBU_SCALE,
            text = plot_data$display_name,
            hovertemplate = paste0(
              "<b>%{text}</b><br>",
              nd$metric_label, ": %{customdata[0]:.4f}<br>",
              "Percentile: %{customdata[1]:.0f}%",
              "<extra></extra>"
            ),
            marker = list(line = list(width = 0.3, color = "#ffffff")),
            colorbar = list(
              title = nd$metric_label,
              tickvals = seq(0, 1, length.out = 5),
              ticktext = sprintf("%.3f",
                quantile(raw_vals, seq(0, 1, length.out = 5), na.rm = TRUE))
            )
          )
      }
      p %>% layout(geo = list(
        scope = "usa",
        projection = list(type = "albers usa"),
        showlakes = FALSE
      ))
    }
  })

  output$nat_cards <- renderUI({
    nd <- nat_data(); req(nd)
    vals <- nd$data[[nd$metric_key]]
    vals <- vals[!is.na(vals)]
    if (length(vals) == 0) return(NULL)

    mc <- function(lbl, val) {
      tags$div(class = "card text-center m-1",
               style = "min-width:80px; flex:1 1 auto;",
        tags$div(class = "card-body py-2",
          tags$small(class = "text-muted", lbl),
          tags$h5(val, style = paste0("color:", RMI_BLUE, ";"))
        ))
    }
    tags$div(class = "d-flex flex-wrap justify-content-center my-3",
      mc("Count", format(length(vals), big.mark = ",")),
      mc("Average", sprintf("%.2f", mean(vals))),
      mc("Median", sprintf("%.2f", median(vals))),
      mc("Range", paste(sprintf("%.2f", min(vals)), "\u2013",
                         sprintf("%.2f", max(vals))))
    )
  })

  output$nat_table <- renderDT({
    nd <- nat_data(); req(nd)
    mk <- nd$metric_key
    tbl <- nd$data %>%
      select(display_name, geoid, all_of(mk)) %>%
      arrange(desc(.data[[mk]]))
    datatable(tbl, colnames = c("Geography", "GeoID", nd$metric_label),
              options = list(pageLength = 20, scrollX = TRUE),
              rownames = FALSE) %>%
      formatRound(mk, 3)
  })

  output$nat_hist <- renderPlotly({
    nd <- nat_data(); req(nd)
    vals <- nd$data[[nd$metric_key]]
    vals <- vals[!is.na(vals)]
    if (length(vals) == 0) return(NULL)
    plot_ly(x = vals, type = "histogram",
            marker = list(color = "#1f78b4",
                          line = list(color = "#a6cee3", width = 1)),
            nbinsx = 40) %>%
      layout(
        title = list(text = paste(nd$metric_label, "Distribution"),
                     font = list(size = 14)),
        xaxis = list(title = nd$metric_label),
        yaxis = list(title = "Count"),
        margin = list(t = 40)
      )
  })

  # ═══════════════════════════════════════════════════════════════════════════
  #  TAB 3: INDUSTRY SPACE
  # ═══════════════════════════════════════════════════════════════════════════
  is_data <- eventReactive(input$is_load, {
    req(meta$loaded)
    level <- input$is_level
    code  <- LEVEL_CODES[[level]]

    withProgress(message = "Loading industry space...", {
      # Lazy-load nodes and edges
      if (is.null(meta$is_nodes)) {
        meta$is_nodes <- fetch_csv("meta/industry_space_nodes.csv")
        incProgress(0.4)
      }
      if (is.null(meta$is_edges)) {
        meta$is_edges <- fetch_csv("meta/industry_space_edges.csv")
        incProgress(0.4)
      }
    })

    nodes <- meta$is_nodes %>%
      filter(geo_aggregation_level == code) %>%
      mutate(industry_code = as.character(industry_code))
    edges <- meta$is_edges %>%
      filter(geo_aggregation_level == code) %>%
      mutate(from = as.character(from), to = as.character(to))

    # Filter edges by weight threshold
    wt <- quantile(edges$weight, input$is_thresh / 100, na.rm = TRUE)
    edges <- edges %>% filter(weight >= wt)

    # Load RCA data if geography selected
    rca_codes <- character(0)
    if (nzchar(input$is_geo)) {
      padded <- pad_geoid(input$is_geo, level)
      geo_data <- fetch_csv_gz(
        paste0("by_geography/", level, "/", padded, ".csv.gz"))
      if (nrow(geo_data) > 0) {
        rca_codes <- as.character(
          geo_data$industry_code[geo_data$industry_comparative_advantage == 1])
      }
    }

    # visNetwork format
    vis_nodes <- nodes %>%
      mutate(
        id = industry_code,
        label = ifelse(nchar(industry_description) > 20,
                       paste0(substr(industry_description, 1, 17), "..."),
                       industry_description),
        title = paste0("<b>", htmltools::htmlEscape(industry_code), "</b><br>",
                       htmltools::htmlEscape(industry_description), "<br>",
                       "Complexity: ", sprintf("%.2f", industry_complexity)),
        value = industry_centrality * 1000,
        color = ifelse(id %in% rca_codes, "#1f78b4", "#cccccc"),
        font.size = 10
      )

    vis_edges <- edges %>%
      select(from, to, weight) %>%
      mutate(
        width = pmax(0.3, weight * 3),
        color = "rgba(150,150,150,0.3)"
      )

    list(nodes = vis_nodes, edges = vis_edges, rca = rca_codes,
         all_nodes = nodes, level = level, geoid = input$is_geo)
  })

  output$is_ui <- renderUI({
    isd <- is_data()
    if (is.null(isd)) {
      return(tags$div(class = "loading-placeholder",
        tags$h4("Industry Space",
                style = paste0("color:", RMI_BLUE, ";")),
        tags$p(class = "text-muted",
               "Select a geography level and click", tags$strong("Load Network."))
      ))
    }

    geo_name <- ""
    if (nzchar(isd$geoid)) {
      geo_name <- paste0(" \u2014 ", geo_display_name(isd$geoid, isd$level))
    }

    tagList(
      tags$h3(paste0("Industry Space", geo_name),
              style = paste0("color:", RMI_BLUE, ";")),
      visNetworkOutput("is_network", height = "550px"),
      fluidRow(
        column(6, plotlyOutput("is_hist", height = "280px")),
        column(6, DTOutput("is_table"))
      )
    )
  })

  output$is_network <- renderVisNetwork({
    isd <- is_data(); req(isd)
    visNetwork(isd$nodes, isd$edges) %>%
      visPhysics(enabled = FALSE) %>%
      visLayout(randomSeed = 42) %>%
      visOptions(highlightNearest = list(enabled = TRUE, degree = 1, hover = TRUE),
                 nodesIdSelection = TRUE) %>%
      visInteraction(navigationButtons = TRUE, zoomView = TRUE) %>%
      visEdges(smooth = FALSE)
  })

  output$is_hist <- renderPlotly({
    isd <- is_data(); req(isd)
    nodes <- isd$all_nodes
    rca <- isd$rca
    if (length(rca) > 0) {
      plot_ly() %>%
        add_histogram(x = nodes$industry_complexity[nodes$industry_code %in% rca],
                      name = "RCA", marker = list(color = "#1f78b4")) %>%
        add_histogram(x = nodes$industry_complexity[!nodes$industry_code %in% rca],
                      name = "Other", marker = list(color = "#cccccc")) %>%
        layout(barmode = "overlay", title = "Industry Complexity",
               xaxis = list(title = "Complexity"),
               yaxis = list(title = "Count"))
    } else {
      plot_ly(x = nodes$industry_complexity, type = "histogram",
              marker = list(color = "#999")) %>%
        layout(title = "Industry Complexity",
               xaxis = list(title = "Complexity"),
               yaxis = list(title = "Count"))
    }
  })

  output$is_table <- renderDT({
    isd <- is_data(); req(isd)
    tbl <- isd$all_nodes %>%
      mutate(rca = ifelse(industry_code %in% isd$rca, "\u2713", "")) %>%
      arrange(desc(industry_complexity)) %>%
      select(industry_code, industry_description, industry_complexity,
             industry_ubiquity, rca)
    datatable(tbl, colnames = c("Code", "Industry", "Complexity", "Ubiquity", "RCA"),
              options = list(pageLength = 15, scrollX = TRUE),
              rownames = FALSE) %>%
      formatRound("industry_complexity", 2)
  })
}

# ══════════════════════════════════════════════════════════════════════════════
#  Helper functions (outside server for clarity)
# ══════════════════════════════════════════════════════════════════════════════
join_geo_names <- function(data, level, meta) {
  code <- LEVEL_CODES[[level]]
  geo_names <- meta$geo %>%
    filter(geo_aggregation_level == code) %>%
    mutate(geoid = pad_geoid(geoid, level))

  if (level == "county" && nrow(meta$crosswalk) > 0) {
    geo_names <- geo_names %>%
      left_join(meta$crosswalk %>% distinct(county_geoid, state_abbreviation),
                by = c("geoid" = "county_geoid")) %>%
      mutate(display_name = paste0(name, ", ", state_abbreviation))
  } else {
    geo_names$display_name <- geo_names$name
  }

  data %>%
    left_join(geo_names %>% select(geoid, display_name), by = "geoid") %>%
    mutate(display_name = ifelse(is.na(display_name), geoid, display_name))
}

expand_to_counties <- function(data, level, metric_key, meta) {
  # Map parent-region values onto constituent counties for choropleth
  parent_col <- switch(level,
    cbsa = "cbsa_geoid", csa = "csa_geoid", cz = "commuting_zone_geoid")
  in_col <- switch(level,
    cbsa = "county_in_cbsa", csa = "county_in_csa", cz = NULL)

  cw <- meta$crosswalk
  if (!is.null(in_col)) {
    cw <- cw %>% filter(.data[[in_col]] %in% c(TRUE, "True"))
  }

  cw <- cw %>%
    mutate(parent_id = as.character(.data[[parent_col]])) %>%
    select(county_geoid, parent_id) %>%
    distinct()

  cw %>%
    left_join(data %>% select(geoid, all_of(metric_key), display_name),
              by = c("parent_id" = "geoid")) %>%
    rename(geoid = county_geoid) %>%
    filter(!is.na(.data[[metric_key]]))
}

# ══════════════════════════════════════════════════════════════════════════════
shinyApp(ui, server)
