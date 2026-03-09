################################################################################
## TAPESTRY ECONOMIC COMPLEXITY PIPELINE — UNIFIED SCRIPT (RESTRUCTURED)
##
## Version: 3.1 (consolidated CSV.gz exports, GitHub pre-flight check)
## Date:    March 2026
##
## This script implements a complete end-to-end pipeline for computing economic
## complexity metrics from Tapestry QCEW employment data (2010-2024) using the
## Daboin et al. methodology (eigenvector method).
##
## KEY ARCHITECTURAL CHANGES (v3.1):
##   - Strict Daboin et al. (2019) methodology: NO minimum ubiquity filtering
##   - Industry space (proximity, density, strategic gain) computed for ALL levels
##   - State v2.4: ECI + density + strategic gain all emp-weighted from county
##   - CONSOLIDATED CSV.gz exports: county data grouped by state (not per-GEOID)
##   - Other geo levels (state, CBSA, CSA, CZ) → single file per level
##   - Industry data → single all_industries.csv.gz per data type
##   - GitHub pre-flight check: detect prior uploads, offer cleanup
##   - GitHub Release fallback for files > 50MB
##   - Memory-efficient: process one year at a time, free after export
##
## Pipeline stages:
##   0. Configuration & package loading
##   1. BLS NAICS hierarchy crosswalk
##   2. Tapestry API authentication & download functions (NO downloading yet)
##   3. NAICS code aggregation rules
##   4. Geographic crosswalk construction (TIGRIS 2024, USDA CZ 2020)
##   5. LODES-based geographic harmonization (CT planning regions, AK Valdez-Cordova)
##   6. Function definitions: process_tapestry_year
##   7. Function definitions: complexity computation
##   8. MAIN LOOP: For each year (reverse order 2024→2010):
##        Step 1. Download 1 year from Tapestry API
##        Step 2. Process it (cleanup, harmonization, LQ)
##        Step 3. Compute complexity at ALL levels (county, CBSA, CSA, CZ)
##        Step 4. Build industry space network (latest year, county-level)
##        Step 5. v2.4 State aggregation (ECI + density + SG from county)
##        Step 6. Assembly (per-level gi_pairs, geo metrics, ind metrics)
##        Step 7. Hierarchical export + GitHub upload + verification
##        Step 8. FREE all year-specific data, call gc()
##   9. Post-loop: export crosswalks, industry space, peer geographies
##  10. README generation
##  11. Final GitHub upload (top-level files), upload summary
##
## Data sources:
##   - Tapestry QCEW (University of Idaho): https://tapestry.nkn.uidaho.edu
##   - BLS NAICS hierarchy crosswalk
##   - Census LODES WAC (CT 2010-2023, AK 2010-2016)
##   - TIGRIS 2024 (counties, CBSAs, CSAs)
##   - USDA ERS 2020 Commuting Zones
##
## GitHub: Auto-uploads to bsf-rmi/RMI_Clean_Growth_Tool
##
## Required packages: httr, jsonlite, readr, dplyr, tidyr, tibble, stringr,
##   readxl, purrr, curl, sf, tigris, arrow, data.table, Matrix, RSpectra,
##   igraph, proxy, base64enc
################################################################################

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 0: CONFIGURATION & PACKAGE LOADING
# ══════════════════════════════════════════════════════════════════════════════

cat("\n")
cat(strrep("=", 80), "\n")
cat("TAPESTRY ECONOMIC COMPLEXITY PIPELINE v3.1 — Daboin Methodology\n")
cat("Timestamp:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat(strrep("=", 80), "\n\n")

# ── User-Configurable Parameters ─────────────────────────────────────────────

START_YEAR   <- 2010L
END_YEAR     <- 2024L
WAGE_RULE    <- 3L
OWNERSHIP    <- "0"
CONVERSION   <- "no_conversion"
# NOTE: No minimum ubiquity filter. Per Daboin et al. (2019), the M matrix is
# simply M[c,i] = 1{RCA >= 1} with no post-hoc filtering of low-ubiquity
# industries. All industries with ubiquity >= 1 (at least one geography
# specializes in them) are retained.

# GitHub configuration
GITHUB_REPO  <- "bsf-rmi/RMI_Clean_Growth_Tool"
GITHUB_FOLDER <- sprintf("Tapestry_Economic_Complexity_%d_%d", START_YEAR, END_YEAR)

# Tapestry API
TAPESTRY_BASE_URL <- "https://tapestry.nkn.uidaho.edu"
MAX_RETRIES       <- 3L
RETRY_DELAY_BASE  <- 5

# Output
VERBOSITY <- 1L

# ── Package Installation & Loading ───────────────────────────────────────────

install_if_missing <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    cat(sprintf("  Installing: %s\n", pkg))
    install.packages(pkg, repos = "https://cloud.r-project.org", quiet = TRUE)
  }
}

required_packages <- c(
  "httr", "jsonlite", "readr", "dplyr", "tidyr", "tibble", "stringr",
  "readxl", "purrr", "curl", "sf", "tigris", "arrow", "data.table",
  "Matrix", "RSpectra", "igraph", "proxy", "base64enc"
)
optional_packages <- c("keyring", "askpass", "ggraph")

cat("[SECTION 0] Checking packages...\n")
invisible(lapply(required_packages, install_if_missing))
invisible(lapply(optional_packages, install_if_missing))

suppressPackageStartupMessages({
  library(httr); library(jsonlite); library(readr); library(dplyr)
  library(tidyr); library(tibble); library(stringr); library(readxl)
  library(purrr); library(curl); library(sf); library(tigris)
  library(arrow); library(data.table); library(Matrix); library(RSpectra)
  library(igraph); library(proxy); library(base64enc)
})

options(
  tigris_use_cache = FALSE, timeout = 600, scipen = 999,
  width = 120, pillar.width = 120
)
sf::sf_use_s2(FALSE)

# ── Utility Functions ────────────────────────────────────────────────────────

dbg <- function(fmt, ...) {
  if (VERBOSITY >= 1) {
    msg <- tryCatch(sprintf(fmt, ...), error = function(e) paste(fmt, collapse = " "))
    cat(format(Sys.time(), "[%H:%M:%S]"), msg, "\n")
  }
}
dbg_detail <- function(fmt, ...) {
  if (VERBOSITY >= 2) dbg(fmt, ...)
}
dbg_debug <- function(fmt, ...) {
  if (VERBOSITY >= 3) dbg(fmt, ...)
}

format_number <- function(x) format(x, big.mark = ",", scientific = FALSE)

format_bytes <- function(bytes) {
  if (is.na(bytes) || bytes < 0) return("unknown")
  if (bytes < 1024) return(sprintf("%d B", bytes))
  if (bytes < 1024^2) return(sprintf("%.1f KB", bytes / 1024))
  if (bytes < 1024^3) return(sprintf("%.1f MB", bytes / 1024^2))
  return(sprintf("%.1f GB", bytes / 1024^3))
}

safe_content_text <- function(resp) {
  tryCatch(
    content(resp, as = "text", encoding = "UTF-8"),
    error = function(e) content(resp, as = "text")
  )
}

# Write a data frame to gzipped CSV (.csv.gz)
# readr natively detects the .gz extension and compresses automatically
write_csv_gz <- function(df, path) {
  readr::write_csv(df, path)
}

`%||%` <- function(x, y) if (is.null(x)) y else x

stress_test <- function(test_name, condition, details = NULL, stop_on_fail = TRUE) {
  if (condition) {
    if (VERBOSITY >= 2) cat("  ✓", test_name, "\n")
    return(invisible(TRUE))
  }
  cat("  ✗ FAIL:", test_name, "\n")
  if (!is.null(details)) cat("    Details:", details, "\n")
  if (stop_on_fail) stop(paste("Stress test failed:", test_name))
  invisible(FALSE)
}

approx_equal <- function(a, b, tol = 1e-10) abs(a - b) < tol

glimpse_data <- function(df, label = "Data") {
  cat(sprintf("  %s: %s rows x %d cols\n", label,
              format_number(nrow(df)), ncol(df)))
  if (nrow(df) > 0) {
    for (cn in names(df)[1:min(5, ncol(df))]) {
      vals <- df[[cn]]
      if (is.numeric(vals)) {
        cat(sprintf("    %-20s: %s to %s (mean=%s)\n", cn,
                    format(min(vals, na.rm = TRUE), big.mark = ","),
                    format(max(vals, na.rm = TRUE), big.mark = ","),
                    format(round(mean(vals, na.rm = TRUE), 2), big.mark = ",")))
      } else {
        cat(sprintf("    %-20s: %d unique values\n", cn, length(unique(vals))))
      }
    }
  }
}

# BLS Excel download with multiple fallback methods
download_bls_excel <- function(url, destfile, timeout_sec = 120) {
  if (file.exists(destfile)) try(unlink(destfile), silent = TRUE)
  ua <- paste0(
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ",
    "AppleWebKit/537.36 (KHTML, like Gecko) ",
    "Chrome/120.0.0.0 Safari/537.36"
  )
  ref <- "https://www.bls.gov/cew/classifications/industry/"
  fname <- basename(url)
  
  is_valid_xlsx <- function(f) {
    if (!file.exists(f)) return(FALSE)
    sz <- file.info(f)$size
    if (is.na(sz) || sz < 100) return(FALSE)
    con <- file(f, "rb")
    on.exit(close(con))
    magic <- readBin(con, "raw", n = 4)
    identical(magic, as.raw(c(0x50, 0x4b, 0x03, 0x04)))
  }
  
  local_candidates <- c(
    file.path(getwd(), fname),
    file.path(dirname(destfile), fname),
    file.path(path.expand("~"), "Downloads", fname),
    file.path(path.expand("~"), "Desktop", fname)
  )
  
  for (lc in local_candidates) {
    if (is_valid_xlsx(lc)) {
      file.copy(lc, destfile, overwrite = TRUE)
      dbg("  Found valid local copy: %s", lc)
      return(invisible(destfile))
    }
  }
  
  dbg("  Attempting BLS download via httr...")
  resp <- tryCatch(
    httr::GET(
      url,
      httr::add_headers(
        `User-Agent` = ua,
        `Accept` = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        `Accept-Language` = "en-US,en;q=0.9",
        `Connection` = "keep-alive",
        `Referer` = ref
      ),
      httr::write_disk(destfile, overwrite = TRUE),
      httr::timeout(timeout_sec)
    ),
    error = function(e) e
  )
  if (!inherits(resp, "error")) {
    sc <- httr::status_code(resp)
    dbg("  httr: HTTP %d", sc)
    if (sc == 200 && is_valid_xlsx(destfile)) {
      return(invisible(destfile))
    }
  } else {
    dbg("  httr error: %s", conditionMessage(resp))
  }
  
  dbg("  Falling back to curl package...")
  if (file.exists(destfile)) try(unlink(destfile), silent = TRUE)
  tryCatch({
    curl::curl_download(
      url, destfile, mode = "wb", quiet = TRUE,
      handle = curl::new_handle(
        useragent = ua,
        referer = ref,
        followlocation = TRUE,
        httpheader = c(
          "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language: en-US,en;q=0.9"
        )
      )
    )
  }, error = function(e) dbg("  curl error: %s", conditionMessage(e)))
  if (is_valid_xlsx(destfile)) return(invisible(destfile))
  
  dbg("  Falling back to download.file...")
  if (file.exists(destfile)) try(unlink(destfile), silent = TRUE)
  for (dl_method in c("libcurl", "auto", "wget", "curl")) {
    tryCatch({
      download.file(
        url, destfile, mode = "wb", quiet = TRUE, method = dl_method
      )
    }, error = function(e) NULL, warning = function(w) NULL)
    if (is_valid_xlsx(destfile)) {
      dbg("  download.file(%s) succeeded", dl_method)
      return(invisible(destfile))
    }
    if (file.exists(destfile)) try(unlink(destfile), silent = TRUE)
  }
  
  dbg("  Falling back to system curl...")
  if (file.exists(destfile)) try(unlink(destfile), silent = TRUE)
  tryCatch({
    system2(
      "curl",
      c(
        "-sL", "--max-time", "120",
        "-o", shQuote(destfile),
        "-H", shQuote(paste0("User-Agent: ", ua)),
        "-H", shQuote(paste0("Referer: ", ref)),
        "-H", shQuote("Accept: text/html,application/xhtml+xml,*/*;q=0.8"),
        "-H", shQuote("Accept-Language: en-US,en;q=0.9"),
        shQuote(url)
      ),
      stdout = FALSE, stderr = FALSE
    )
  }, error = function(e) NULL)
  if (is_valid_xlsx(destfile)) return(invisible(destfile))
  
  dbg("  Falling back to system wget...")
  if (file.exists(destfile)) try(unlink(destfile), silent = TRUE)
  tryCatch({
    system2(
      "wget",
      c(
        "-q", "-O", shQuote(destfile),
        paste0("--user-agent=", shQuote(ua)),
        paste0("--referer=", shQuote(ref)),
        shQuote(url)
      ),
      stdout = FALSE, stderr = FALSE
    )
  }, error = function(e) NULL)
  if (is_valid_xlsx(destfile)) return(invisible(destfile))
  
  user_save_path <- file.path(path.expand("~"), "Downloads", fname)
  cat("\n")
  cat(strrep("-", 70), "\n")
  cat("MANUAL DOWNLOAD REQUIRED\n")
  cat("The BLS website blocked all automated download attempts.\n\n")
  cat("Please open this URL in your browser:\n")
  cat("  ", url, "\n\n")
  cat("Save the file to your Downloads folder as:\n")
  cat("  ", user_save_path, "\n")
  cat(strrep("-", 70), "\n")
  cat("Press ENTER after saving (or type 'exit' to quit): ")
  
  ans <- readline()
  if (tolower(trimws(ans)) == "exit") {
    stop("Script terminated by user.", call. = FALSE)
  }
  
  search_paths <- c(
    destfile, user_save_path, local_candidates,
    file.path(path.expand("~"), "Downloads", fname),
    file.path(path.expand("~"), "Desktop", fname),
    file.path(getwd(), fname)
  )
  for (sp in unique(search_paths)) {
    if (is_valid_xlsx(sp)) {
      if (sp != destfile) file.copy(sp, destfile, overwrite = TRUE)
      dbg("  Found file at: %s", sp)
      return(invisible(destfile))
    }
  }
  
  stop(paste0(
    "Could not find a valid NAICS hierarchy crosswalk file.\n",
    "Please download from:\n  ", url, "\n",
    "And place in one of these locations:\n",
    "  ", user_save_path, "\n",
    "  ", file.path(getwd(), fname), "\n"
  ))
}

# LODES gzipped CSV reader
read_gz <- function(url) {
  tf <- tempfile(fileext = ".csv.gz")
  curl::curl_download(url, tf, quiet = TRUE)
  readr::read_csv(tf, show_col_types = FALSE, progress = FALSE,
                  col_types = readr::cols(w_geocode = readr::col_character(), .default = readr::col_double()))
}

# Clean commuting zone names
clean_cz_name <- function(x) {
  x <- stringr::str_squish(as.character(x))
  m <- stringr::str_match(x, "^(.*),\\s*(.+)$")
  place <- ifelse(is.na(m[, 2]), x, m[, 2])
  st <- ifelse(is.na(m[, 3]), NA_character_, m[, 3]) %>%
    stringr::str_replace_all("--", "-") %>%
    stringr::str_replace_all("\\s*-\\s*", "-") %>%
    stringr::str_squish()
  city_pattern <- paste0(
    "\\s+(city and borough|consolidated government \\(balance\\)",
    "|city|town|village|CDP)\\s*$"
  )
  place <- place %>%
    stringr::str_replace(stringr::regex(city_pattern, TRUE), "") %>%
    stringr::str_squish()
  stringr::str_squish(stringr::str_replace(
    ifelse(is.na(st), place, paste0(place, ", ", st)), ",\\s+", ", "))
}

# Parallel configuration
n_cores <- max(1, parallel::detectCores() - 1)
use_parallel <- n_cores > 1 && .Platform$OS.type != "windows"

# Timing
pipeline_start_time <- Sys.time()
section_timers <- list()
start_section <- function(name) {
  section_timers[[name]] <<- Sys.time()
  dbg("=== %s ===", name)
}
end_section <- function(name) {
  elapsed <- as.numeric(difftime(Sys.time(), section_timers[[name]], units = "secs"))
  dbg("  [%s completed in %.1f seconds]", name, elapsed)
}

# Create timestamped output folder
timestamp_str <- format(Sys.time(), "%Y%m%d_%H%M%S")
output_folder <- file.path(tempdir(), paste0("tapestry_complexity_", timestamp_str))
dir.create(output_folder, showWarnings = FALSE, recursive = TRUE)
chunk_folder <- file.path(output_folder, "chunks")
dir.create(chunk_folder, showWarnings = FALSE, recursive = TRUE)
dbg("Output folder: %s", output_folder)
dbg("Chunk folder: %s", chunk_folder)

cat(sprintf("  Configuration complete. Years: %d-%d | Wage Rule: %d | Repo: %s\n",
            START_YEAR, END_YEAR, WAGE_RULE, GITHUB_REPO))

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1: BLS NAICS HIERARCHY CROSSWALK
# ══════════════════════════════════════════════════════════════════════════════

start_section("SECTION 1: NAICS Hierarchy Crosswalk")

naics_xwalk_url  <- "https://www.bls.gov/cew/classifications/industry/qcew-naics-hierarchy-crosswalk.xlsx"
naics_xwalk_path <- file.path(tempdir(), "qcew-naics-hierarchy-crosswalk.xlsx")
download_bls_excel(naics_xwalk_url, naics_xwalk_path)

naics_hierarchy_xwalk <- purrr::map_dfr(
  readxl::excel_sheets(naics_xwalk_path),
  ~ readxl::read_excel(naics_xwalk_path, sheet = .x) %>%
    dplyr::mutate(naics_year = .x) %>%
    dplyr::mutate(across(everything(), as.character))
)
dbg("NAICS hierarchy crosswalk: %s rows", format_number(nrow(naics_hierarchy_xwalk)))

NAICS_VERSION_MAP <- tibble::tribble(
  ~year_min, ~year_max, ~naics_version,
  2022L, 2099L, "v2022",
  2017L, 2021L, "v2017",
  2012L, 2016L, "v2012",
  2007L, 2011L, "v2007",
  2000L, 2006L, "v2002"
)

get_naics_version <- function(year) {
  for (i in seq_len(nrow(NAICS_VERSION_MAP))) {
    if (year >= NAICS_VERSION_MAP$year_min[i] && year <= NAICS_VERSION_MAP$year_max[i])
      return(NAICS_VERSION_MAP$naics_version[i])
  }
  "v2022"
}

naics6_to_sector_mapping <- naics_hierarchy_xwalk %>%
  dplyr::select(naics6_code, sector_code, sector_title, naics_year) %>%
  dplyr::filter(!is.na(naics6_code), !is.na(sector_code)) %>%
  dplyr::distinct(naics6_code, .keep_all = TRUE) %>%
  dplyr::mutate(
    sector_code_for_allocation = dplyr::if_else(naics6_code == "999999", "00", sector_code)
  ) %>%
  dplyr::select(naics6_code, sector_code, sector_title, sector_code_for_allocation)

dbg("NAICS6->sector mapping: %d unique codes", nrow(naics6_to_sector_mapping))

naics_title_lookups <- list()
for (nv in c("v2022", "v2017", "v2012", "v2007")) {
  vd <- naics_hierarchy_xwalk %>% dplyr::filter(naics_year == nv)
  if (nrow(vd) == 0) next
  naics_title_lookups[[nv]] <- vd %>%
    dplyr::select(naics6_code, naics6_title) %>%
    dplyr::filter(!is.na(naics6_code), !is.na(naics6_title)) %>%
    dplyr::distinct(naics6_code, .keep_all = TRUE)
}

end_section("SECTION 1: NAICS Hierarchy Crosswalk")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2: TAPESTRY API AUTHENTICATION & FUNCTION DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

start_section("SECTION 2: Tapestry API Setup (Definitions Only)")

# ── Credentials ──────────────────────────────────────────────────────────────

get_tapestry_credentials <- function() {
  service <- "tapestry-uidaho"
  have_keyring <- requireNamespace("keyring", quietly = TRUE)
  
  if (have_keyring) {
    kr <- tryCatch(keyring::key_list(service = service), error = function(e) data.frame())
    if (nrow(kr) > 0) {
      email <- kr$username[1]
      dbg("Found stored credentials in keyring for: %s", email)
      return(list(email = email, password = keyring::key_get(service, email)))
    }
  }
  
  cat("\n")
  email <- readline("Tapestry email (or 'exit' to quit): ")
  if (tolower(trimws(email)) == "exit") stop("Script terminated by user.", call. = FALSE)
  
  if (requireNamespace("askpass", quietly = TRUE)) {
    password <- askpass::askpass("Tapestry password: ")
  } else {
    password <- readline("Tapestry password: ")
  }
  
  if (have_keyring) {
    try(keyring::key_set_with_value(service, email, password), silent = TRUE)
    dbg("Credentials stored in keyring for future use.")
  }
  list(email = email, password = password)
}

# ── Session & Login ──────────────────────────────────────────────────────────

tapestry_handle <- httr::handle(TAPESTRY_BASE_URL)

login_tapestry <- function(email, password, max_attempts = 3) {
  dbg("Authenticating with Tapestry API...")
  
  login_page <- tryCatch(
    httr::GET(handle = tapestry_handle, path = "/login"),
    error = function(e) {
      dbg("  Cannot reach Tapestry login page: %s", e$message)
      return(NULL)
    }
  )
  if (is.null(login_page)) return(FALSE)
  dbg("  Login page: HTTP %d", httr::status_code(login_page))
  
  resp <- tryCatch(
    httr::POST(
      handle = tapestry_handle, path = "/login",
      body = list(email = email, password = password), encode = "form"
    ),
    error = function(e) {
      dbg("  POST /login failed: %s", e$message)
      return(NULL)
    }
  )
  if (is.null(resp)) return(FALSE)
  dbg("  POST /login: HTTP %d", httr::status_code(resp))
  
  check <- tryCatch(
    httr::GET(handle = tapestry_handle, path = "/data"),
    error = function(e) NULL
  )
  if (!is.null(check) &&
      httr::status_code(check) == 200 &&
      !grepl("Login", safe_content_text(check))) {
    dbg("Login successful!")
    return(TRUE)
  }
  
  dbg("  Login verification failed — credentials may be incorrect.")
  return(FALSE)
}

# ── Download Function ────────────────────────────────────────────────────────

qcew_col_types <- readr::cols(
  own_code         = readr::col_character(),
  year             = readr::col_integer(),
  area_fips        = readr::col_character(),
  naics_code       = readr::col_character(),
  tap_estabs_count = readr::col_double(),
  .default         = readr::col_double()
)

download_tapestry_year <- function(year, wage_rule = WAGE_RULE, max_retries = MAX_RETRIES) {
  dbg("  Downloading year %d (Rule %d)...", year, wage_rule)
  
  payload <- list(
    area_fips       = list("All"),
    own_code        = as.character(OWNERSHIP),
    industry_code   = list("All"),
    emp_wage_number = as.character(wage_rule),
    fips_calc       = "fipsInd",
    naics_calc      = "naicsInd",
    year            = as.character(year),
    sector_conversion = CONVERSION
  )
  
  for (attempt in seq_len(max_retries)) {
    if (attempt > 1) {
      delay <- RETRY_DELAY_BASE * (2 ^ (attempt - 2))
      dbg("    Retry %d/%d in %d seconds...", attempt, max_retries, delay)
      Sys.sleep(delay)
    }
    
    tmp_zip <- tempfile(fileext = ".zip")
    
    resp <- tryCatch(
      httr::POST(
        url = paste0(TAPESTRY_BASE_URL, "/download-data"),
        body = payload, encode = "json",
        httr::add_headers(
          Accept = "application/json",
          `Content-Type` = "application/json",
          `X-Requested-With` = "XMLHttpRequest"),
        httr::write_disk(tmp_zip, overwrite = TRUE),
        httr::timeout(600),
        handle = tapestry_handle),
      error = function(e) { dbg("    Request failed: %s", e$message); NULL })
    
    if (is.null(resp)) { if (file.exists(tmp_zip)) unlink(tmp_zip); next }
    
    if (httr::status_code(resp) >= 500) {
      dbg("    Server error HTTP %d", httr::status_code(resp))
      unlink(tmp_zip); next
    }
    if (httr::status_code(resp) != 200) {
      dbg("    HTTP %d — not retrying", httr::status_code(resp))
      unlink(tmp_zip); return(NULL)
    }
    
    ct <- httr::headers(resp)[["content-type"]]
    if (is.null(ct) || !grepl("zip", ct, ignore.case = TRUE)) {
      dbg("    Not a ZIP response"); unlink(tmp_zip); return(NULL)
    }
    
    exdir <- tempfile()
    dir.create(exdir)
    files <- unzip(tmp_zip, exdir = exdir)
    csvs <- files[grepl("\\.csv$", files, ignore.case = TRUE)]
    
    if (length(csvs) == 0) { unlink(tmp_zip); unlink(exdir, TRUE); next }
    
    df <- tryCatch(
      readr::read_csv(csvs[1], show_col_types = FALSE, progress = FALSE,
                      col_types = qcew_col_types),
      error = function(e) { dbg("    CSV read error: %s", e$message); NULL })
    
    unlink(tmp_zip); unlink(exdir, TRUE)
    
    if (!is.null(df) && nrow(df) > 0) {
      dbg("    Year %d: %s rows downloaded", year, format_number(nrow(df)))
      return(df)
    }
  }
  
  warning(sprintf("Failed to download year %d after %d attempts", year, max_retries))
  return(NULL)
}

# ── Execute LOGIN only (NOT downloads) ────────────────────────────────────────

creds <- get_tapestry_credentials()
login_ok <- login_tapestry(creds$email, creds$password)
if (!isTRUE(login_ok)) {
  dbg("Login failed. Requesting credentials again...")
  creds <- get_tapestry_credentials()
  login_ok <- login_tapestry(creds$email, creds$password)
  if (!isTRUE(login_ok)) {
    stop("Tapestry login failed after 2 attempts — check credentials.", call. = FALSE)
  }
}

end_section("SECTION 2: Tapestry API Setup")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3: NAICS CODE AGGREGATION RULES
# ══════════════════════════════════════════════════════════════════════════════

start_section("SECTION 3: NAICS Aggregation Rules")

three_digit_rollups <- list(
  list(target = "812000", title = "Personal and Laundry Services",
       pattern = "^812[0-9]{3}$"),
  list(target = "111000", title = "Crop Production",
       pattern = "^111[0-9]{3}$"),
  list(target = "112000", title = "Animal Production and Aquaculture",
       pattern = "^112[0-9]{3}$"),
  list(target = "513190", title = "Other Publishers",
       pattern = "^51319[0-9]$")
)

bls_contractor_mappings <- tibble::tribble(
  ~source_code, ~target_code, ~target_title,
  "238111", "238110", "Poured Concrete Foundation and Structure Contractors",
  "238112", "238110", "Poured Concrete Foundation and Structure Contractors",
  "238121", "238120", "Structural Steel and Precast Concrete Contractors",
  "238122", "238120", "Structural Steel and Precast Concrete Contractors",
  "238131", "238130", "Framing Contractors",
  "238132", "238130", "Framing Contractors",
  "238141", "238140", "Masonry Contractors",
  "238142", "238140", "Masonry Contractors",
  "238151", "238150", "Glass and Glazing Contractors",
  "238152", "238150", "Glass and Glazing Contractors",
  "238161", "238160", "Roofing Contractors",
  "238162", "238160", "Roofing Contractors",
  "238171", "238170", "Siding Contractors",
  "238172", "238170", "Siding Contractors",
  "238191", "238190", "Other Foundation, Structure, and Building Exterior Contractors",
  "238192", "238190", "Other Foundation, Structure, and Building Exterior Contractors",
  "238211", "238210", "Electrical Contractors and Other Wiring Installation Contractors",
  "238212", "238210", "Electrical Contractors and Other Wiring Installation Contractors",
  "238221", "238220", "Plumbing, Heating, and Air-Conditioning Contractors",
  "238222", "238220", "Plumbing, Heating, and Air-Conditioning Contractors",
  "238291", "238290", "Other Building Equipment Contractors",
  "238292", "238290", "Other Building Equipment Contractors",
  "238311", "238310", "Drywall and Insulation Contractors",
  "238312", "238310", "Drywall and Insulation Contractors",
  "238321", "238320", "Painting and Wall Covering Contractors",
  "238322", "238320", "Painting and Wall Covering Contractors",
  "238331", "238330", "Flooring Contractors",
  "238332", "238330", "Flooring Contractors",
  "238341", "238340", "Tile and Terrazzo Contractors",
  "238342", "238340", "Tile and Terrazzo Contractors",
  "238351", "238350", "Finish Carpentry Contractors",
  "238352", "238350", "Finish Carpentry Contractors",
  "238391", "238390", "Other Building Finishing Contractors",
  "238392", "238390", "Other Building Finishing Contractors",
  "238911", "238910", "Site Preparation Contractors",
  "238912", "238910", "Site Preparation Contractors",
  "238991", "238990", "All Other Specialty Trade Contractors",
  "238992", "238990", "All Other Specialty Trade Contractors"
)

bls_recode_vec <- setNames(
  bls_contractor_mappings$target_code,
  bls_contractor_mappings$source_code
)

for (nv in c("v2022", "v2017", "v2012", "v2007")) {
  for (rollup in three_digit_rollups) {
    existing <- naics_hierarchy_xwalk %>%
      dplyr::filter(naics_year == nv, naics6_code == rollup$target)
    if (nrow(existing) == 0) {
      sample_row <- naics_hierarchy_xwalk %>%
        dplyr::filter(naics_year == nv, stringr::str_detect(naics6_code, rollup$pattern)) %>%
        dplyr::slice(1)
      if (nrow(sample_row) > 0) {
        synthetic <- tibble::tibble(
          naics6_code = rollup$target, naics6_title = rollup$title,
          naics5_code = substr(rollup$target, 1, 5),
          naics5_title = rollup$title,
          naics4_code = substr(rollup$target, 1, 4),
          naics4_title = rollup$title,
          naics3_code = substr(rollup$target, 1, 3),
          naics3_title = rollup$title,
          sector_code = sample_row$sector_code,
          sector_title = sample_row$sector_title,
          naics_year = nv
        )
        missing_cols <- setdiff(names(naics_hierarchy_xwalk), names(synthetic))
        for (mc in missing_cols) synthetic[[mc]] <- NA_character_
        naics_hierarchy_xwalk <- dplyr::bind_rows(naics_hierarchy_xwalk, synthetic)
      }
    }
  }
}

apply_naics_aggregation <- function(dt) {
  n_bls <- sum(dt$naics6_code %in% names(bls_recode_vec))
  if (n_bls > 0) {
    dt <- dt %>%
      dplyr::mutate(naics6_code = dplyr::if_else(
        naics6_code %in% names(bls_recode_vec), bls_recode_vec[naics6_code], naics6_code))
    dbg_detail("  BLS contractor recode: %d records", n_bls)
  }
  
  for (rollup in three_digit_rollups) {
    match_idx <- stringr::str_detect(dt$naics6_code, rollup$pattern) &
      dt$naics6_code != rollup$target
    n_match <- sum(match_idx, na.rm = TRUE)
    if (n_match > 0) {
      dt <- dt %>%
        dplyr::mutate(naics6_code = dplyr::if_else(
          stringr::str_detect(naics6_code, rollup$pattern) & naics6_code != rollup$target,
          rollup$target, naics6_code))
      dbg_detail("  Rollup %s (%s): %d records", rollup$target, rollup$title, n_match)
    }
  }
  
  dt
}

dbg("NAICS aggregation rules defined: %d BLS mappings, %d rollups",
    nrow(bls_contractor_mappings), length(three_digit_rollups))

end_section("SECTION 3: NAICS Aggregation Rules")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4: GEOGRAPHIC CROSSWALK CONSTRUCTION
# ══════════════════════════════════════════════════════════════════════════════

start_section("SECTION 4: Geographic Crosswalk")

dbg("Loading TIGRIS 2024 counties, CBSAs, CSAs, states...")

states_data <- tigris::states(year = 2024) %>%
  sf::st_drop_geometry() %>%
  dplyr::filter(!STATEFP %in% c("60", "66", "69", "72", "78")) %>%
  dplyr::transmute(
    state_fips = STATEFP, state_name = NAME,
    state_abbreviation = STUSPS
  )

tigris_counties <- tigris::counties(year = 2024, cb = FALSE) %>%
  dplyr::filter(!STATEFP %in% c("60", "66", "69", "72", "78"))

cbsa_data <- tigris::core_based_statistical_areas(year = 2024) %>%
  sf::st_drop_geometry() %>%
  dplyr::transmute(cbsa_geoid = CBSAFP, cbsa_name = NAMELSAD)

csa_data <- tigris::combined_statistical_areas(year = 2024) %>%
  sf::st_drop_geometry() %>%
  dplyr::transmute(csa_geoid = CSAFP, csa_name = NAMELSAD)

dbg("Loading USDA ERS 2020 Commuting Zone crosswalk...")

cz_url <- "https://www.ers.usda.gov/media/6968/2020-commuting-zones.csv?v=56155"
cz_raw <- readr::read_csv(cz_url, show_col_types = FALSE)

county_cz_xwalk <- cz_raw %>%
  dplyr::transmute(
    county_geoid = FIPStxt,
    commuting_zone_2020 = as.integer(CZ2020),
    cz_name = clean_cz_name(CZName)
  )

dbg("  CZ crosswalk: %d counties, %d unique CZs",
    nrow(county_cz_xwalk), dplyr::n_distinct(county_cz_xwalk$commuting_zone_2020))

county_crosswalk <- tigris_counties %>%
  dplyr::select(STATEFP, COUNTYFP, GEOID, NAMELSAD, CBSAFP, CSAFP, geometry) %>%
  dplyr::rename(
    state_fips = STATEFP, county_fips = COUNTYFP, county_geoid = GEOID,
    county_name = NAMELSAD, cbsa_geoid = CBSAFP, csa_geoid = CSAFP) %>%
  dplyr::mutate(
    county_in_cbsa = !is.na(cbsa_geoid) & cbsa_geoid != "",
    county_in_csa  = !is.na(csa_geoid) & csa_geoid != "") %>%
  dplyr::left_join(cbsa_data, by = "cbsa_geoid") %>%
  dplyr::left_join(csa_data, by = "csa_geoid") %>%
  dplyr::left_join(states_data, by = "state_fips") %>%
  dplyr::left_join(county_cz_xwalk, by = "county_geoid")

county_geometry <- county_crosswalk %>% dplyr::select(county_geoid, geometry)

county_crosswalk_df <- county_crosswalk %>% sf::st_drop_geometry()

dbg("Integrated crosswalk: %d counties | %d in CBSA | %d in CSA | %d with CZ",
    nrow(county_crosswalk_df),
    sum(county_crosswalk_df$county_in_cbsa),
    sum(county_crosswalk_df$county_in_csa),
    sum(!is.na(county_crosswalk_df$commuting_zone_2020)))

end_section("SECTION 4: Geographic Crosswalk")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5: LODES-BASED GEOGRAPHIC HARMONIZATION
# ══════════════════════════════════════════════════════════════════════════════

start_section("SECTION 5: LODES Harmonization (CT + AK)")

NAICS_SECTOR_MAP <- tibble::tribble(
  ~cns_code, ~naics_sector, ~sector_name,
  "C000",  "00",    "Total, All Industries",
  "CNS01", "11",    "Agriculture, Forestry, Fishing and Hunting",
  "CNS02", "21",    "Mining, Quarrying, and Oil and Gas Extraction",
  "CNS03", "22",    "Utilities",
  "CNS04", "23",    "Construction",
  "CNS05", "31-33", "Manufacturing",
  "CNS06", "42",    "Wholesale Trade",
  "CNS07", "44-45", "Retail Trade",
  "CNS08", "48-49", "Transportation and Warehousing",
  "CNS09", "51",    "Information",
  "CNS10", "52",    "Finance and Insurance",
  "CNS11", "53",    "Real Estate and Rental and Leasing",
  "CNS12", "54",    "Professional, Scientific, and Technical Services",
  "CNS13", "55",    "Management of Companies and Enterprises",
  "CNS14", "56",    "Admin Support and Waste Mgmt and Remediation",
  "CNS15", "61",    "Educational Services",
  "CNS16", "62",    "Health Care and Social Assistance",
  "CNS17", "71",    "Arts, Entertainment, and Recreation",
  "CNS18", "72",    "Accommodation and Food Services",
  "CNS19", "81",    "Other Services (except Public Administration)",
  "CNS20", "92",    "Public Administration"
)

cns_rename_map <- setNames(NAICS_SECTOR_MAP$cns_code, NAICS_SECTOR_MAP$naics_sector)

build_lodes_allocation_crosswalk <- function(
    state_abbr_lodes, state_abbr_tigris, lodes_years,
    target_crs, old_sf, new_sf, label) {
  
  dbg("  [%s] Downloading Census blocks...", label)
  blocks_raw <- tigris::blocks(state = state_abbr_tigris, year = 2020, class = "sf")
  blk_col <- intersect(c("GEOID20", "GEOID10", "GEOID"), names(blocks_raw))[1]
  
  census_blocks <- blocks_raw %>%
    dplyr::select(all_of(blk_col), geometry) %>%
    dplyr::rename(block_geoid = all_of(blk_col)) %>%
    dplyr::mutate(block_geoid = as.character(block_geoid)) %>%
    sf::st_transform(crs = target_crs)
  
  dbg("  [%s] %d blocks loaded", label, nrow(census_blocks))
  
  dbg("  [%s] Downloading LODES WAC data (%d-%d)...", label,
      min(lodes_years), max(lodes_years))
  wac_url_base <- sprintf("https://lehd.ces.census.gov/data/lodes/LODES8/%s/wac/",
                          tolower(state_abbr_lodes))
  
  wac_all <- dplyr::bind_rows(lapply(lodes_years, function(yr) {
    f <- sprintf("%s_wac_S000_JT00_%d.csv.gz", tolower(state_abbr_lodes), yr)
    w <- tryCatch(read_gz(paste0(wac_url_base, f)), error = function(e) NULL)
    if (is.null(w) || !("w_geocode" %in% names(w))) return(NULL)
    cols_keep <- c("w_geocode", "C000", paste0("CNS", sprintf("%02d", 1:20)))
    w %>%
      dplyr::select(all_of(intersect(cols_keep, names(w)))) %>%
      dplyr::rename(block_geoid = w_geocode) %>%
      dplyr::mutate(block_geoid = as.character(block_geoid), year = as.integer(yr))
  }))
  
  present_cns <- intersect(as.character(cns_rename_map), names(wac_all))
  rename_filtered <- cns_rename_map[cns_rename_map %in% present_cns]
  wac_all <- wac_all %>% dplyr::rename(!!!rename_filtered)
  
  dbg("  [%s] Computing block-to-geography assignments...", label)
  block_centroids <- census_blocks %>%
    suppressWarnings(sf::st_centroid(of_largest_polygon = TRUE))
  
  blocks_to_old <- block_centroids %>%
    sf::st_join(old_sf, join = sf::st_within, left = TRUE) %>%
    sf::st_drop_geometry() %>%
    dplyr::select(block_geoid, old_county_geoid)
  
  blocks_to_new <- block_centroids %>%
    sf::st_join(new_sf, join = sf::st_within, left = TRUE) %>%
    sf::st_drop_geometry() %>%
    dplyr::select(block_geoid, new_region_geoid)
  
  block_geo <- blocks_to_old %>%
    dplyr::inner_join(blocks_to_new, by = "block_geoid")
  
  dbg("  [%s] %d blocks with dual geography assignment", label, nrow(block_geo))
  
  sector_cols <- NAICS_SECTOR_MAP$naics_sector[NAICS_SECTOR_MAP$naics_sector %in% names(wac_all)]
  valid_blocks <- block_geo$block_geoid
  
  panel <- tidyr::expand_grid(block_geoid = valid_blocks, year = lodes_years)
  wac_complete <- panel %>%
    dplyr::left_join(
      wac_all %>% dplyr::select(block_geoid, year, all_of(sector_cols)),
      by = c("block_geoid", "year")
    )
  for (col in sector_cols) wac_complete[[col]][is.na(wac_complete[[col]])] <- 0
  
  wac_with_geo <- wac_complete %>% dplyr::left_join(block_geo, by = "block_geoid")
  
  dbg("  [%s] Computing allocation factors...", label)
  all_xwalks <- list()
  for (yr in lodes_years) {
    for (sector in sector_cols) {
      yd <- wac_with_geo %>% dplyr::filter(year == yr)
      xw <- yd %>%
        dplyr::group_by(old_county_geoid, new_region_geoid) %>%
        dplyr::summarise(sector_jobs = sum(.data[[sector]], na.rm = TRUE), .groups = "drop") %>%
        dplyr::group_by(old_county_geoid) %>%
        dplyr::mutate(
          total = sum(sector_jobs, na.rm = TRUE),
          afact = dplyr::if_else(total > 0, sector_jobs / total, NA_real_)) %>%
        dplyr::ungroup() %>%
        dplyr::mutate(year = yr, naics_sector = sector) %>%
        dplyr::select(year, from_geoid = old_county_geoid, to_geoid = new_region_geoid,
                      naics_sector, afact)
      all_xwalks[[length(all_xwalks) + 1]] <- xw
    }
  }
  
  result <- dplyr::bind_rows(all_xwalks) %>%
    dplyr::arrange(year, naics_sector, from_geoid, to_geoid)
  dbg("  [%s] Allocation crosswalk: %d rows", label, nrow(result))
  result
}

dbg("Building Connecticut allocation crosswalk...")

ct_old_counties <- tigris::counties(state = "CT", year = 2020, class = "sf") %>%
  dplyr::select(GEOID, NAMELSAD) %>%
  dplyr::rename(old_county_geoid = GEOID, old_county_name = NAMELSAD) %>%
  sf::st_transform(crs = 5070)

ct_new_regions <- tigris::counties(state = "CT", year = 2024, class = "sf") %>%
  dplyr::select(GEOID, NAMELSAD) %>%
  dplyr::rename(new_region_geoid = GEOID, new_region_name = NAMELSAD) %>%
  sf::st_transform(crs = 5070)

ct_allocation_xwalk <- build_lodes_allocation_crosswalk(
  state_abbr_lodes = "ct", state_abbr_tigris = "CT",
  lodes_years = 2010:2023, target_crs = 5070,
  old_sf = ct_old_counties, new_sf = ct_new_regions, label = "CT")

dbg("Building Alaska Valdez-Cordova allocation crosswalk...")

ak_old_vc <- tigris::counties(state = "AK", year = 2019, class = "sf") %>%
  dplyr::filter(GEOID == "02261") %>%
  dplyr::select(GEOID, NAMELSAD) %>%
  dplyr::rename(old_county_geoid = GEOID, old_county_name = NAMELSAD) %>%
  sf::st_transform(crs = 3338)

ak_new_regions <- tigris::counties(state = "AK", year = 2024, class = "sf") %>%
  dplyr::filter(GEOID %in% c("02063", "02066")) %>%
  dplyr::select(GEOID, NAMELSAD) %>%
  dplyr::rename(new_region_geoid = GEOID, new_region_name = NAMELSAD) %>%
  sf::st_transform(crs = 3338)

ak_allocation_xwalk <- build_lodes_allocation_crosswalk(
  state_abbr_lodes = "ak", state_abbr_tigris = "AK",
  lodes_years = 2010:2016, target_crs = 3338,
  old_sf = ak_old_vc, new_sf = ak_new_regions, label = "AK")

end_section("SECTION 5: LODES Harmonization (CT + AK)")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6: FUNCTION DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

start_section("SECTION 6: Function Definitions")

# Constants for geographic conversions
CT_OLD_COUNTIES <- c("09001", "09003", "09005", "09007", "09009", "09011", "09013", "09015")
AK_VALDEZ_CORDOVA <- "02261"

# [KEEP process_tapestry_year FUNCTION EXACTLY AS-IS FROM ORIGINAL]
process_tapestry_year <- function(yr, raw_df) {
  dbg("Processing year %d...", yr)
  
  emp_col <- paste0("tap_emplvl_est_", WAGE_RULE)
  if (!emp_col %in% names(raw_df)) {
    emp_candidates <- grep("^tap_emplvl_est_", names(raw_df), value = TRUE)
    if (length(emp_candidates) == 0) {
      stop(sprintf("Year %d: no tap_emplvl_est_* column found in data", yr))
    }
    emp_col <- emp_candidates[1]
    dbg("  [WARN] Column tap_emplvl_est_%d not found, using %s", WAGE_RULE, emp_col)
  }
  
  dt <- raw_df %>%
    dplyr::transmute(
      year = as.integer(year),
      county_geoid = stringr::str_pad(
        as.character(area_fips), width = 5, side = "left", pad = "0"
      ),
      naics6_code = as.character(naics_code),
      employment = as.numeric(.data[[emp_col]])
    ) %>%
    dplyr::filter(!is.na(employment))
  
  has_wh <- any(dt$county_geoid == "02270")
  has_kv <- any(dt$county_geoid == "02158")
  if (yr == 2015 && has_wh && has_kv) {
    dt <- dt %>% dplyr::filter(county_geoid != "02270")
  } else if (has_wh && !has_kv) {
    dt <- dt %>% dplyr::mutate(county_geoid = dplyr::if_else(
      county_geoid == "02270", "02158", county_geoid))
  }
  dt <- dt %>% dplyr::mutate(county_geoid = dplyr::if_else(
    county_geoid == "46113", "46102", county_geoid))
  dt <- dt %>% dplyr::mutate(county_geoid = dplyr::if_else(
    county_geoid == "51515", "51019", county_geoid))
  
  dt <- dt %>%
    dplyr::group_by(year, county_geoid, naics6_code) %>%
    dplyr::summarise(employment = sum(employment, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(
      state_fips = stringr::str_sub(county_geoid, 1, 2),
      county_fips_3 = stringr::str_sub(county_geoid, 3, 5),
      is_unknown_county = (county_fips_3 == "999")
    )
  
  dt <- apply_naics_aggregation(dt)
  
  dt <- dt %>%
    dplyr::group_by(year, county_geoid, naics6_code, state_fips, county_fips_3, is_unknown_county) %>%
    dplyr::summarise(employment = sum(employment, na.rm = TRUE), .groups = "drop")
  
  if (yr >= 2010 && yr <= 2023) {
    ct_rows <- dt %>% dplyr::filter(county_geoid %in% CT_OLD_COUNTIES, !is_unknown_county)
    if (nrow(ct_rows) > 0) {
      ct_unknown <- dt %>% dplyr::filter(state_fips == "09", is_unknown_county)
      non_ct <- dt %>% dplyr::filter(!(state_fips == "09") | (state_fips == "09" & is_unknown_county))
      
      ct_rows <- ct_rows %>%
        dplyr::left_join(naics6_to_sector_mapping %>%
                           dplyr::select(naics6_code, sector_code_for_allocation),
                         by = "naics6_code") %>%
        dplyr::mutate(sector_code_for_allocation = dplyr::if_else(
          is.na(sector_code_for_allocation), "00", sector_code_for_allocation))
      
      ct_xw <- ct_allocation_xwalk %>%
        dplyr::filter(year == !!yr) %>%
        dplyr::select(from_geoid, to_geoid, naics_sector, afact)
      ct_fb <- ct_xw %>% dplyr::filter(naics_sector == "00") %>%
        dplyr::select(from_geoid, to_geoid, afact_fb = afact)
      
      ct_with_factors <- ct_rows %>%
        dplyr::left_join(ct_xw,
                         by = c("county_geoid" = "from_geoid", "sector_code_for_allocation" = "naics_sector"),
                         relationship = "many-to-many")
      
      if (any(is.na(ct_with_factors$afact))) {
        ct_with_factors <- ct_with_factors %>%
          dplyr::left_join(ct_fb, by = c("county_geoid" = "from_geoid", "to_geoid")) %>%
          dplyr::mutate(afact = dplyr::if_else(is.na(afact), afact_fb, afact)) %>%
          dplyr::select(-afact_fb)
      }
      
      ct_converted <- ct_with_factors %>%
        dplyr::filter(!is.na(afact), !is.na(to_geoid)) %>%
        dplyr::mutate(
          employment = round(employment * afact),
          county_geoid = to_geoid,
          state_fips = stringr::str_sub(to_geoid, 1, 2),
          county_fips_3 = stringr::str_sub(to_geoid, 3, 5)) %>%
        dplyr::select(year, county_geoid, naics6_code, employment,
                      state_fips, county_fips_3, is_unknown_county)
      
      dt <- dplyr::bind_rows(non_ct, ct_unknown, ct_converted)
      dbg_detail("  CT conversion: %d -> %d rows", nrow(ct_rows), nrow(ct_converted))
    }
  }
  
  if (yr >= 2010 && yr <= 2019) {
    vc_rows <- dt %>% dplyr::filter(county_geoid == AK_VALDEZ_CORDOVA)
    if (nrow(vc_rows) > 0) {
      non_vc <- dt %>% dplyr::filter(county_geoid != AK_VALDEZ_CORDOVA)
      
      ak_lookup_yr <- if (yr > 2016) 2016L else as.integer(yr)
      
      vc_rows <- vc_rows %>%
        dplyr::left_join(naics6_to_sector_mapping %>%
                           dplyr::select(naics6_code, sector_code_for_allocation),
                         by = "naics6_code") %>%
        dplyr::mutate(sector_code_for_allocation = dplyr::if_else(
          is.na(sector_code_for_allocation), "00", sector_code_for_allocation))
      
      ak_xw <- ak_allocation_xwalk %>%
        dplyr::filter(year == ak_lookup_yr) %>%
        dplyr::select(from_geoid, to_geoid, naics_sector, afact)
      ak_fb <- ak_xw %>% dplyr::filter(naics_sector == "00") %>%
        dplyr::select(from_geoid, to_geoid, afact_fb = afact)
      
      vc_with_factors <- vc_rows %>%
        dplyr::left_join(ak_xw,
                         by = c("county_geoid" = "from_geoid", "sector_code_for_allocation" = "naics_sector"),
                         relationship = "many-to-many")
      
      if (any(is.na(vc_with_factors$afact))) {
        vc_with_factors <- vc_with_factors %>%
          dplyr::left_join(ak_fb, by = c("county_geoid" = "from_geoid", "to_geoid")) %>%
          dplyr::mutate(afact = dplyr::if_else(is.na(afact), afact_fb, afact)) %>%
          dplyr::select(-afact_fb)
      }
      
      vc_converted <- vc_with_factors %>%
        dplyr::filter(!is.na(afact), !is.na(to_geoid)) %>%
        dplyr::mutate(
          employment = round(employment * afact),
          county_geoid = to_geoid,
          state_fips = stringr::str_sub(to_geoid, 1, 2),
          county_fips_3 = stringr::str_sub(to_geoid, 3, 5)) %>%
        dplyr::select(year, county_geoid, naics6_code, employment,
                      state_fips, county_fips_3, is_unknown_county)
      
      dt <- dplyr::bind_rows(non_vc, vc_converted)
      dbg_detail("  AK VC conversion: %d -> %d rows", nrow(vc_rows), nrow(vc_converted))
    }
  }
  
  dt <- dt %>%
    dplyr::group_by(year, county_geoid, naics6_code) %>%
    dplyr::summarise(employment = sum(employment, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(
      state_fips = stringr::str_sub(county_geoid, 1, 2),
      county_fips_3 = stringr::str_sub(county_geoid, 3, 5),
      is_unknown_county = (county_fips_3 == "999")
    )
  
  dt <- dt %>%
    dplyr::filter(
      !is_unknown_county,
      naics6_code != "999999"
    )
  
  dt <- dt %>%
    dplyr::left_join(
      county_crosswalk_df %>% dplyr::select(
        county_geoid, county_name, state_fips, state_name, state_abbreviation,
        cbsa_geoid, cbsa_name, csa_geoid, csa_name,
        commuting_zone_2020, cz_name),
      by = c("county_geoid", "state_fips"))
  
  national_total <- sum(dt$employment, na.rm = TRUE)
  national_by_industry <- dt %>%
    dplyr::group_by(naics6_code) %>%
    dplyr::summarise(national_emp = sum(employment, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(national_share = national_emp / national_total)
  
  county_totals <- dt %>%
    dplyr::group_by(county_geoid) %>%
    dplyr::summarise(county_total_emp = sum(employment, na.rm = TRUE), .groups = "drop")
  
  county_lq <- dt %>%
    dplyr::left_join(county_totals, by = "county_geoid") %>%
    dplyr::left_join(national_by_industry, by = "naics6_code") %>%
    dplyr::mutate(
      county_share = employment / county_total_emp,
      location_quotient = dplyr::if_else(
        national_share > 0, county_share / national_share, 0))
  
  aggregate_to_level <- function(data, geo_col, geo_name_col) {
    geo_col_sym <- rlang::sym(geo_col)
    geo_name_sym <- rlang::sym(geo_name_col)
    
    agg <- data %>%
      dplyr::filter(!is.na(!!geo_col_sym), !!geo_col_sym != "") %>%
      dplyr::group_by(!!geo_col_sym, !!geo_name_sym, naics6_code) %>%
      dplyr::summarise(employment = sum(employment, na.rm = TRUE), .groups = "drop")
    
    geo_totals <- agg %>%
      dplyr::group_by(!!geo_col_sym) %>%
      dplyr::summarise(geo_total_emp = sum(employment, na.rm = TRUE), .groups = "drop")
    
    agg %>%
      dplyr::left_join(geo_totals, by = geo_col) %>%
      dplyr::left_join(national_by_industry, by = "naics6_code") %>%
      dplyr::mutate(
        geo_share = employment / geo_total_emp,
        location_quotient = dplyr::if_else(
          national_share > 0, geo_share / national_share, 0)) %>%
      dplyr::rename(geoid = !!geo_col_sym, geo_name = !!geo_name_sym)
  }
  
  state_data <- dt %>%
    dplyr::group_by(state_fips, state_name, naics6_code) %>%
    dplyr::summarise(employment = sum(employment, na.rm = TRUE), .groups = "drop")
  state_totals <- state_data %>%
    dplyr::group_by(state_fips) %>%
    dplyr::summarise(geo_total_emp = sum(employment, na.rm = TRUE), .groups = "drop")
  state_lq <- state_data %>%
    dplyr::left_join(state_totals, by = "state_fips") %>%
    dplyr::left_join(national_by_industry, by = "naics6_code") %>%
    dplyr::mutate(
      geo_share = employment / geo_total_emp,
      location_quotient = dplyr::if_else(national_share > 0, geo_share / national_share, 0)) %>%
    dplyr::rename(geoid = state_fips, geo_name = state_name)
  
  cbsa_lq <- aggregate_to_level(county_lq, "cbsa_geoid", "cbsa_name")
  csa_lq  <- aggregate_to_level(county_lq, "csa_geoid", "csa_name")
  
  cz_data <- dt %>%
    dplyr::filter(!is.na(commuting_zone_2020)) %>%
    dplyr::mutate(cz_geoid = as.character(commuting_zone_2020)) %>%
    dplyr::group_by(cz_geoid, cz_name, naics6_code) %>%
    dplyr::summarise(employment = sum(employment, na.rm = TRUE), .groups = "drop")
  cz_totals <- cz_data %>%
    dplyr::group_by(cz_geoid) %>%
    dplyr::summarise(geo_total_emp = sum(employment, na.rm = TRUE), .groups = "drop")
  cz_lq <- cz_data %>%
    dplyr::left_join(cz_totals, by = "cz_geoid") %>%
    dplyr::left_join(national_by_industry, by = "naics6_code") %>%
    dplyr::mutate(
      geo_share = employment / geo_total_emp,
      location_quotient = dplyr::if_else(national_share > 0, geo_share / national_share, 0)) %>%
    dplyr::rename(geoid = cz_geoid, geo_name = cz_name)
  
  county_lq_formatted <- county_lq %>%
    dplyr::rename(geoid = county_geoid, geo_name = county_name,
                  geo_total_emp = county_total_emp, geo_share = county_share)
  
  dbg("  Year %d: %s county-industry pairs | National emp: %s",
      yr, format_number(nrow(county_lq)), format_number(national_total))
  
  list(
    year = yr,
    county = county_lq_formatted,
    state  = state_lq,
    cbsa   = cbsa_lq,
    csa    = csa_lq,
    cz     = cz_lq,
    national_by_industry = national_by_industry,
    national_total = national_total
  )
}

dbg("process_tapestry_year function defined")

end_section("SECTION 6: Function Definitions")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7: COMPLEXITY COMPUTATION FUNCTIONS (Daboin et al. methodology)
# ══════════════════════════════════════════════════════════════════════════════

start_section("SECTION 7: Complexity Functions")

estimate_memory_gb <- function(n_rows, n_cols, bytes = 8) {
  (n_rows * n_cols * bytes) / (1024^3)
}

get_available_memory_gb <- function() {
  tryCatch({
    if (.Platform$OS.type == "unix") {
      mi <- system("free -b 2>/dev/null || vm_stat 2>/dev/null", intern = TRUE)
      if (length(mi) > 0 && grepl("free", mi[1], ignore.case = TRUE))
        return(as.numeric(strsplit(mi[2], "\\s+")[[1]][7]) / (1024^3))
    }
    8
  }, error = function(e) 8)
}

calc_chunk_size <- function(total_size, mem_per_elem_gb, max_frac = 0.5) {
  avail <- get_available_memory_gb() * max_frac
  max(100, min(floor(sqrt(avail / mem_per_elem_gb)), total_size))
}

should_chunk <- function(n_elements, size_threshold = 5000) {
  me <- estimate_memory_gb(n_elements, n_elements, 8)
  (n_elements > size_threshold) || (me > get_available_memory_gb() * 0.3)
}

compute_proximity_vectorized <- function(M) {
  U <- crossprod(M)
  d <- diag(U)
  mx <- pmax(outer(d, rep(1, length(d))), outer(rep(1, length(d)), d))
  phi <- U / mx
  phi[is.nan(phi)] <- 0
  list(U = U, phi = phi)
}

compute_proximity_chunked <- function(M, chunk_size = NULL) {
  ni <- ncol(M)
  if (is.null(chunk_size)) chunk_size <- calc_chunk_size(ni, (8 * 3) / (1024^3), 0.3)
  if (ni <= chunk_size) return(compute_proximity_vectorized(M))
  
  U <- matrix(0, ni, ni)
  nc <- ceiling(ni / chunk_size)
  for (ic in 1:nc) {
    is_ <- (ic - 1) * chunk_size + 1; ie <- min(ic * chunk_size, ni)
    for (jc in ic:nc) {
      js <- (jc - 1) * chunk_size + 1; je <- min(jc * chunk_size, ni)
      Uc <- crossprod(M[, is_:ie, drop = FALSE], M[, js:je, drop = FALSE])
      U[is_:ie, js:je] <- Uc
      if (ic != jc) U[js:je, is_:ie] <- t(Uc)
    }
  }
  
  d <- diag(U)
  phi <- matrix(0, ni, ni)
  for (ic in 1:nc) {
    is_ <- (ic - 1) * chunk_size + 1; ie <- min(ic * chunk_size, ni); ir <- is_:ie
    for (jc in ic:nc) {
      js <- (jc - 1) * chunk_size + 1; je <- min(jc * chunk_size, ni); jr <- js:je
      mx <- pmax(outer(d[ir], rep(1, length(jr))), outer(rep(1, length(ir)), d[jr]))
      pc <- U[ir, jr] / mx; pc[is.nan(pc)] <- 0
      phi[ir, jr] <- pc
      if (ic != jc) phi[jr, ir] <- t(pc)
    }
  }
  list(U = U, phi = phi)
}

compute_M_tilde_chunked <- function(M, row_scale, col_scale, chunk_size = NULL) {
  nr <- nrow(M)
  if (is.null(chunk_size)) chunk_size <- calc_chunk_size(nr, (8 * 2) / (1024^3), 0.3)
  if (nr <= chunk_size) {
    Mcs <- t(t(M) / col_scale)
    return((Mcs / row_scale) %*% t(M))
  }
  Mt <- matrix(0, nr, nr)
  nc <- ceiling(nr / chunk_size)
  for (ic in 1:nc) {
    is_ <- (ic - 1) * chunk_size + 1; ie <- min(ic * chunk_size, nr); ir <- is_:ie
    Mc <- M[ir, , drop = FALSE]
    Mcs <- t(t(Mc) / col_scale)
    Mt[ir, ] <- (Mcs / row_scale[ir]) %*% t(M)
  }
  Mt
}

compute_strategic_gain <- function(
    M, phi, density_mat, ici, phi_col_sums,
    chunk_size = 500) {
  ng <- nrow(M); ni <- ncol(M)
  phi_norm <- t(t(phi) / phi_col_sums)
  
  if (ng <= chunk_size) {
    ici_m <- matrix(ici, nrow = ng, ncol = ni, byrow = TRUE)
    return(((1 - M) * ici_m) %*% t(phi_norm) - density_mat * ici_m)
  }
  
  sg <- matrix(0, ng, ni)
  nc <- ceiling(ng / chunk_size)
  for (ic in 1:nc) {
    is_ <- (ic - 1) * chunk_size + 1; ie <- min(ic * chunk_size, ng); ir <- is_:ie
    cn <- length(ir)
    Mc <- M[ir, , drop = FALSE]; dc <- density_mat[ir, , drop = FALSE]
    im <- matrix(ici, nrow = cn, ncol = ni, byrow = TRUE)
    sg[ir, ] <- ((1 - Mc) * im) %*% t(phi_norm) - dc * im
  }
  sg
}

create_edge_list <- function(phi, ind_codes, top_pct = 0.05) {
  ut <- which(upper.tri(phi), arr.ind = TRUE)
  w <- phi[ut]
  el <- data.frame(
    from = ind_codes[ut[, 1]], to = ind_codes[ut[, 2]],
    weight = w, stringsAsFactors = FALSE
  )
  el <- el[el$weight > 0, ]
  nt <- ceiling(nrow(el) * top_pct)
  thr <- sort(el$weight, decreasing = TRUE)[min(nt, nrow(el))]
  top_e <- el[el$weight >= thr, ]
  sf <- el %>% dplyr::group_by(from) %>%
    dplyr::slice_max(weight, n = 1, with_ties = FALSE) %>% dplyr::ungroup()
  st <- el %>% dplyr::group_by(to) %>%
    dplyr::slice_max(weight, n = 1, with_ties = FALSE) %>% dplyr::ungroup()
  dplyr::bind_rows(top_e, sf, st) %>% dplyr::distinct(from, to, .keep_all = TRUE)
}

compute_complexity <- function(
    lq_data, geo_level_name,
    create_industry_space = TRUE,
    run_stress_tests = TRUE) {
  dbg("  Computing complexity for %s...", geo_level_name)
  
  wide <- lq_data %>%
    dplyr::select(geoid, naics6_code, location_quotient) %>%
    dplyr::distinct(geoid, naics6_code, .keep_all = TRUE) %>%
    tidyr::pivot_wider(
      names_from = naics6_code, values_from = location_quotient,
      values_fill = 0
    )
  
  geo_ids <- wide$geoid
  ind_codes <- setdiff(names(wide), "geoid")
  M <- as.matrix(wide[, ind_codes])
  M <- ifelse(M >= 1, 1, 0)
  rownames(M) <- geo_ids; colnames(M) <- ind_codes
  
  # Per Daboin et al. (2019): NO minimum ubiquity filter.
  # M[c,i] = 1{RCA >= 1} is used directly. Industries with ubiquity = 0
  # (no geography specializes) are dropped since they contribute nothing.
  ubiquity_vec <- colSums(M)
  keep_inds <- ubiquity_vec >= 1
  n_dropped <- sum(!keep_inds)
  if (n_dropped > 0) {
    dbg_detail("    Dropped %d industries with ubiquity = 0 (no geography specializes)", n_dropped)
    M <- M[, keep_inds, drop = FALSE]
    ind_codes <- colnames(M)
    ubiquity_vec <- colSums(M)
  }
  
  diversity_vec <- rowSums(M)
  keep_geos <- diversity_vec > 0
  if (any(!keep_geos)) {
    dbg_detail("    Removed %d geographies with zero diversity", sum(!keep_geos))
    M <- M[keep_geos, , drop = FALSE]
    geo_ids <- rownames(M)
    diversity_vec <- rowSums(M)
  }
  
  n_geos <- nrow(M); n_inds <- ncol(M)
  fill_rate <- mean(M)
  
  dbg("    M matrix: %d geos x %d industries | fill = %.4f", n_geos, n_inds, fill_rate)
  
  if (n_geos < 3 || n_inds < 3) {
    dbg("    [SKIP] Too few geographies or industries for complexity computation")
    return(NULL)
  }
  
  K_c1 <- (M %*% ubiquity_vec) / diversity_vec
  K_i1 <- (t(M) %*% diversity_vec) / ubiquity_vec
  
  use_chunked <- should_chunk(n_geos, 3000)
  if (use_chunked) {
    M_tilde_C <- compute_M_tilde_chunked(M, diversity_vec, ubiquity_vec)
  } else {
    Mcs <- t(t(M) / ubiquity_vec)
    M_tilde_C <- (Mcs / diversity_vec) %*% t(M)
  }
  
  if (run_stress_tests) {
    stress_test("M_tilde_C rows sum to 1", all(approx_equal(rowSums(M_tilde_C), 1)))
    stress_test("M_tilde_C non-negative", all(M_tilde_C >= 0))
    stress_test("M is binary (0/1 only)", all(M %in% c(0, 1)))
    stress_test("diversity > 0 for all retained geos", all(diversity_vec > 0))
    stress_test("ubiquity > 0 for all retained industries", all(ubiquity_vec > 0))
  }
  
  eigen_C <- eigen(M_tilde_C, symmetric = FALSE)
  vals_C <- Re(eigen_C$values)
  ord_C <- order(vals_C, decreasing = TRUE)
  second_eval_C <- vals_C[ord_C[2]]
  third_eval_C <- vals_C[ord_C[3]]
  eci_raw <- Re(eigen_C$vectors[, ord_C[2]])
  
  dbg("    Eigenvalues C: l2=%.6f | l3=%.6f | gap=%.6f",
      second_eval_C, third_eval_C, second_eval_C - third_eval_C)
  
  use_chunked_I <- should_chunk(n_inds, 2000)
  if (use_chunked_I) {
    M_tilde_I <- compute_M_tilde_chunked(t(M), ubiquity_vec, diversity_vec)
  } else {
    Mrs <- M / diversity_vec
    M_tilde_I <- (t(Mrs) / ubiquity_vec) %*% M
  }
  
  if (run_stress_tests) {
    stress_test("M_tilde_I rows sum to 1", all(approx_equal(rowSums(M_tilde_I), 1)))
    stress_test("M_tilde_I non-negative", all(M_tilde_I >= 0))
  }
  
  eigen_I <- eigen(M_tilde_I, symmetric = FALSE)
  vals_I <- Re(eigen_I$values)
  ord_I <- order(vals_I, decreasing = TRUE)
  second_eval_I <- vals_I[ord_I[2]]
  ici_raw <- Re(eigen_I$vectors[, ord_I[2]])
  
  dbg("    Shared lambda2: C=%.8f | I=%.8f | diff=%.1e",
      second_eval_C, second_eval_I, abs(second_eval_C - second_eval_I))
  
  if (cor(ici_raw, ubiquity_vec) > 0) ici_raw <- -ici_raw
  if (cor(eci_raw, diversity_vec) < 0) eci_raw <- -eci_raw
  
  eci <- (eci_raw - mean(eci_raw)) / sd(eci_raw)
  ici <- (ici_raw - mean(ici_raw)) / sd(ici_raw)
  
  avg_ici_by_geo <- vapply(seq_len(n_geos), function(c_idx) {
    specs <- which(M[c_idx, ] == 1)
    if (length(specs) == 0) NA_real_ else mean(ici[specs])
  }, numeric(1))
  
  eci_avgici_corr <- cor(eci, avg_ici_by_geo, use = "complete.obs")
  if (eci_avgici_corr < 0) {
    eci <- -eci
    eci_avgici_corr <- -eci_avgici_corr
    dbg("    [SIGN FIX] ECI re-oriented by avg_ICI criterion")
  }
  
  dbg("    cor(ECI,div)=%.3f | cor(ICI,ubiq)=%.3f | cor(ECI,avgICI)=%.3f",
      cor(eci, diversity_vec), cor(ici, ubiquity_vec), eci_avgici_corr)
  
  if (run_stress_tests) {
    stress_test(
      "Shared eigenvalues close (C vs I)",
      abs(second_eval_C - second_eval_I) < 1e-6,
      sprintf("diff = %.2e", abs(second_eval_C - second_eval_I)),
      stop_on_fail = FALSE
    )
    stress_test(
      "ECI-avgICI correlation > 0 (sign coherent)",
      eci_avgici_corr > 0,
      sprintf("cor = %.4f", eci_avgici_corr),
      stop_on_fail = FALSE
    )
    stress_test("No NA in ECI", !any(is.na(eci)))
    stress_test("No NA in ICI", !any(is.na(ici)))
  }
  
  proximity_results <- NULL
  if (create_industry_space) {
    prox <- if (should_chunk(n_inds, 2000))
      compute_proximity_chunked(M) else compute_proximity_vectorized(M)
    phi <- prox$phi
    rownames(phi) <- colnames(phi) <- ind_codes
    
    if (run_stress_tests) {
      stress_test("phi symmetric", isSymmetric(phi, tol = 1e-10))
      stress_test("phi diagonal = 1", all(approx_equal(diag(phi), 1)))
      stress_test("phi in [0,1]", all(phi >= 0) && all(phi <= 1 + 1e-10))
    }
    
    phi_col_sums <- colSums(phi)
    centrality <- rowSums(phi) / sum(phi)
    
    density_mat <- (M %*% phi) / matrix(
      phi_col_sums, nrow = n_geos, ncol = n_inds, byrow = TRUE
    )
    rownames(density_mat) <- geo_ids; colnames(density_mat) <- ind_codes
    
    ici_mat <- matrix(ici, nrow = n_geos, ncol = n_inds, byrow = TRUE)
    absence <- 1 - M
    strategic_index <- rowSums(density_mat * absence * ici_mat)
    names(strategic_index) <- geo_ids
    
    sg <- compute_strategic_gain(M, phi, density_mat, ici, phi_col_sums)
    rownames(sg) <- geo_ids; colnames(sg) <- ind_codes
    
    proximity_results <- list(
      co_occurrence = prox$U, proximity = phi,
      centrality = centrality, density = density_mat,
      strategic_index = strategic_index, strategic_gain = sg)
  }
  
  list(
    eci = tibble(geoid = geo_ids, economic_complexity_index = eci),
    ici = tibble(industry_code = ind_codes, industry_complexity_index = ici),
    diversity = tibble(geoid = geo_ids, diversity = diversity_vec),
    ubiquity = tibble(industry_code = ind_codes, ubiquity = ubiquity_vec),
    K_c1 = tibble(geoid = geo_ids, avg_ubiquity = as.vector(K_c1)),
    K_i1 = tibble(industry_code = ind_codes, avg_diversity = as.vector(K_i1)),
    avg_ici = tibble(geoid = geo_ids, avg_ici = avg_ici_by_geo),
    M_matrix = M,
    proximity = proximity_results,
    diagnostics = list(
      n_geos = n_geos, n_inds = n_inds, fill_rate = fill_rate,
      second_eigenvalue_C = second_eval_C, second_eigenvalue_I = second_eval_I,
      eigenvalue_gap_C = second_eval_C - third_eval_C,
      eci_diversity_corr = cor(eci, diversity_vec),
      ici_ubiquity_corr = cor(ici, ubiquity_vec),
      eci_avgici_corr = eci_avgici_corr,
      n_dropped_zero_ubiquity = n_dropped)
  )
}

compute_peer_geographies <- function(M, n_peers = 5) {
  sim <- as.matrix(proxy::simil(M, method = "Jaccard"))
  rownames(sim) <- colnames(sim) <- rownames(M)
  
  geo_ids <- rownames(M)
  all_peers <- lapply(geo_ids, function(gid) {
    s <- sim[gid, ]; s <- s[names(s) != gid]
    top <- sort(s, decreasing = TRUE)[1:min(n_peers, length(s))]
    data.frame(
      geoid = gid, peer_geoid = names(top),
      jaccard_similarity = as.vector(top),
      peer_rank = seq_along(top), stringsAsFactors = FALSE
    )
  })
  dplyr::bind_rows(all_peers)
}

dbg("Complexity functions defined")
end_section("SECTION 7: Complexity Functions")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8: YEAR-BY-YEAR MAIN LOOP (MEMORY-EFFICIENT RESTRUCTURING)
# ══════════════════════════════════════════════════════════════════════════════

start_section("SECTION 8: Main Loop — Year-by-Year Processing")

# Get NAICS title lookup for current data
industry_names_lookup <- naics_title_lookups[["v2022"]] %>%
  dplyr::rename(industry_code = naics6_code, industry_description = naics6_title)

# Storage for cumulative results (across all years)
# NOTE: geo_industry_pairs are written to parquet CHUNKS on disk (too large for memory)
all_geo_metrics_list <- list()
all_ind_metrics_list <- list()
all_diagnostics_list <- list()
all_peer_data_list <- list()

# Create temp directory for gi_pairs parquet chunks
gi_chunk_dir <- file.path(output_folder, "_gi_chunks")
dir.create(gi_chunk_dir, showWarnings = FALSE, recursive = TRUE)
gi_chunk_count <- 0
gi_total_rows <- 0

# Industry space (from latest year only)
industry_space_edges <- NULL
industry_space_nodes <- NULL

# ── GitHub setup (needed for per-year uploads inside the loop) ──────────────

github_token <- Sys.getenv("GITHUB_PAT")
if (github_token == "") github_token <- Sys.getenv("GITHUB_TOKEN")

github_ready <- (github_token != "")
if (!github_ready) {
  dbg("[WARN] No GitHub token found (GITHUB_PAT or GITHUB_TOKEN). Per-year uploads DISABLED.")
  dbg("  Files will still be saved locally. Set GITHUB_PAT and re-run.")
} else {
  dbg("GitHub token found. Per-year uploads ENABLED.")
}

github_api_base <- sprintf("https://api.github.com/repos/%s", GITHUB_REPO)

gh_headers <- function() {
  httr::add_headers(
    Authorization = paste("token", github_token),
    Accept = "application/vnd.github.v3+json",
    `Content-Type` = "application/json"
  )
}

upload_file_to_github <- function(local_path, github_path, commit_msg = NULL) {
  if (!github_ready) return(FALSE)
  file_content <- base64enc::base64encode(local_path)
  file_size <- file.size(local_path)
  if (is.null(commit_msg)) commit_msg <- sprintf("Add %s", basename(github_path))
  
  check_url <- paste0(github_api_base, "/contents/", github_path)
  existing <- tryCatch(httr::GET(check_url, gh_headers()), error = function(e) NULL)
  
  sha <- NULL
  if (!is.null(existing) && httr::status_code(existing) == 200) {
    sha <- httr::content(existing)$sha
  }
  
  body <- list(message = commit_msg, content = file_content, branch = "main")
  if (!is.null(sha)) body$sha <- sha
  
  resp <- httr::PUT(
    check_url,
    body = jsonlite::toJSON(body, auto_unbox = TRUE),
    gh_headers(),
    httr::timeout(300)
  )
  
  status <- httr::status_code(resp)
  if (status %in% c(200, 201)) {
    dbg("    [GH-OK] Uploaded: %s (%s)", github_path, format_bytes(file_size))
    return(TRUE)
  } else {
    msg <- tryCatch(httr::content(resp, as = "text", encoding = "UTF-8"), error = function(e) "")
    dbg("    [GH-FAIL] %s: HTTP %d — %s", github_path, status, substr(msg, 1, 200))
    return(FALSE)
  }
}

verify_github_file <- function(github_path) {
  if (!github_ready) return(FALSE)
  check_url <- paste0(github_api_base, "/contents/", github_path)
  resp <- tryCatch(httr::GET(check_url, gh_headers()), error = function(e) NULL)
  if (!is.null(resp) && httr::status_code(resp) == 200) {
    info <- httr::content(resp)
    return(list(
      exists = TRUE,
      name = info$name,
      size = info$size,
      sha = info$sha
    ))
  }
  return(list(exists = FALSE))
}

show_github_folder_structure <- function(folder_path) {
  if (!github_ready) return(invisible(NULL))
  url <- paste0(github_api_base, "/contents/", folder_path)
  resp <- tryCatch(httr::GET(url, gh_headers()), error = function(e) NULL)
  
  if (is.null(resp) || httr::status_code(resp) != 200) {
    dbg("    [GH-TREE] Could not list %s (HTTP %s)",
        folder_path,
        if (!is.null(resp)) httr::status_code(resp) else "error")
    return(invisible(NULL))
  }
  
  items <- httr::content(resp)
  if (length(items) == 0) {
    dbg("    [GH-TREE] %s/ (empty)", folder_path)
    return(invisible(NULL))
  }
  
  cat("\n")
  cat(sprintf("    ┌─ GitHub: %s/%s/ (%d files)\n", GITHUB_REPO, folder_path, length(items)))
  for (i in seq_along(items)) {
    item <- items[[i]]
    connector <- if (i == length(items)) "└──" else "├──"
    size_str <- if (!is.null(item$size) && item$size > 0) {
      sprintf(" (%s)", format_bytes(item$size))
    } else ""
    cat(sprintf("    %s %s%s\n", connector, item$name, size_str))
  }
  cat("\n")
  
  return(invisible(items))
}

# Track per-year upload results
year_upload_log <- list()

# ── PRE-FLIGHT: Check GitHub for prior Tapestry upload attempts ─────────────
# SAFETY: This ONLY inspects and cleans up files within GITHUB_FOLDER
# (i.e. "Tapestry_Economic_Complexity_2010_2024/"). It will never touch
# any other folder or file in the repository.

if (github_ready) {
  dbg("Checking GitHub for existing Tapestry output in %s/%s/ ...", GITHUB_REPO, GITHUB_FOLDER)
  
  # Recursive function to list all files in a GitHub folder.
  # GUARD: only follows paths that begin with the expected root_prefix.
  list_github_folder_recursive <- function(folder_path, root_prefix = GITHUB_FOLDER) {
    # Safety: refuse to enumerate outside the Tapestry output folder
    if (!startsWith(folder_path, root_prefix)) {
      dbg("  [SAFETY] Skipping unexpected path outside Tapestry folder: %s", folder_path)
      return(character(0))
    }
    url <- paste0(github_api_base, "/contents/", folder_path)
    resp <- tryCatch(httr::GET(url, gh_headers()), error = function(e) NULL)
    if (is.null(resp) || httr::status_code(resp) != 200) return(character(0))
    items <- httr::content(resp)
    if (length(items) == 0) return(character(0))
    paths <- c()
    for (item in items) {
      if (item$type == "dir") {
        paths <- c(paths, list_github_folder_recursive(item$path, root_prefix))
      } else if (startsWith(item$path, root_prefix)) {
        paths <- c(paths, item$path)
      } else {
        dbg("  [SAFETY] Skipping file outside Tapestry folder: %s", item$path)
      }
    }
    return(paths)
  }
  
  # Delete a single file from GitHub by path.
  # GUARD: refuses to delete anything outside GITHUB_FOLDER.
  delete_github_file <- function(file_path) {
    if (!startsWith(file_path, GITHUB_FOLDER)) {
      dbg("  [SAFETY] Refusing to delete path outside Tapestry folder: %s", file_path)
      return(FALSE)
    }
    url <- paste0(github_api_base, "/contents/", file_path)
    resp <- tryCatch(httr::GET(url, gh_headers()), error = function(e) NULL)
    if (is.null(resp) || httr::status_code(resp) != 200) return(FALSE)
    sha <- httr::content(resp)$sha
    body <- list(
      message = sprintf("Remove %s (Tapestry pre-flight cleanup)", basename(file_path)),
      sha = sha, branch = "main")
    del_resp <- tryCatch(
      httr::DELETE(url,
                   body = jsonlite::toJSON(body, auto_unbox = TRUE),
                   gh_headers(), httr::timeout(120)),
      error = function(e) NULL)
    if (!is.null(del_resp) && httr::status_code(del_resp) %in% c(200, 204)) {
      return(TRUE)
    }
    return(FALSE)
  }
  
  # Check if the Tapestry output folder exists on GitHub
  preflight_url <- paste0(github_api_base, "/contents/", GITHUB_FOLDER)
  preflight_resp <- tryCatch(httr::GET(preflight_url, gh_headers()), error = function(e) NULL)
  
  if (!is.null(preflight_resp) && httr::status_code(preflight_resp) == 200) {
    preflight_items <- httr::content(preflight_resp)
    n_top_items <- length(preflight_items)
    
    # Count dirs vs files at top level
    n_dirs <- sum(sapply(preflight_items, function(x) x$type == "dir"))
    n_files <- n_top_items - n_dirs
    
    cat("\n")
    cat(strrep("!", 70), "\n")
    cat(sprintf("  PRIOR TAPESTRY OUTPUT DETECTED in %s/%s/\n", GITHUB_REPO, GITHUB_FOLDER))
    cat(sprintf("  Top-level items: %d folders, %d files\n", n_dirs, n_files))
    cat(sprintf("  (Only this Tapestry folder will be affected — nothing else in the repo)\n"))
    cat(strrep("!", 70), "\n\n")
    
    # Show top-level listing
    for (item in preflight_items) {
      type_str <- if (item$type == "dir") "[DIR]" else sprintf("[%s]", format_bytes(item$size))
      cat(sprintf("    %s %s\n", type_str, item$name))
    }
    cat("\n")
    
    # Interactive prompt
    preflight_action <- readline(prompt = paste0(
      "Action? [c]lear Tapestry folder / [k]eep (uploads will overwrite) / [a]bort: "))
    
    if (tolower(preflight_action) %in% c("a", "abort")) {
      cat("\n  Pipeline aborted by user. No changes made.\n")
      stop("User aborted pipeline at pre-flight check.", call. = FALSE)
    } else if (tolower(preflight_action) %in% c("c", "clear")) {
      cat(sprintf("\n  Enumerating all files in %s/ for deletion...\n", GITHUB_FOLDER))
      all_gh_files <- list_github_folder_recursive(GITHUB_FOLDER)
      cat(sprintf("  Found %d files to delete (all within %s/).\n",
                  length(all_gh_files), GITHUB_FOLDER))
      
      if (length(all_gh_files) > 0) {
        confirm <- readline(prompt = sprintf(
          "  Confirm: delete ALL %d files from %s/%s/? Type 'DELETE' to confirm: ",
          length(all_gh_files), GITHUB_REPO, GITHUB_FOLDER))
        
        if (confirm == "DELETE") {
          del_ok <- 0; del_fail <- 0
          for (i in seq_along(all_gh_files)) {
            if (i %% 50 == 0 || i == length(all_gh_files)) {
              cat(sprintf("    Deleting... %d/%d\r", i, length(all_gh_files)))
            }
            ok <- delete_github_file(all_gh_files[i])
            if (ok) del_ok <- del_ok + 1 else del_fail <- del_fail + 1
            # Rate limit: GitHub allows ~5,000 requests/hour; pace deletions
            if (i %% 20 == 0) Sys.sleep(1)
          }
          cat(sprintf("\n  Tapestry cleanup complete: %d deleted, %d failed.\n\n",
                      del_ok, del_fail))
          if (del_fail > 0) {
            dbg("[WARN] %d files could not be deleted. They will be overwritten if names match.",
                del_fail)
          }
        } else {
          cat("  Deletion cancelled. Continuing — uploads will overwrite existing files.\n\n")
        }
      }
    } else {
      cat("  Keeping existing content. Uploads will overwrite files with matching names.\n\n")
    }
  } else {
    dbg("No existing Tapestry output found at %s/%s/ — clean slate.",
        GITHUB_REPO, GITHUB_FOLDER)
  }
}

# Process years in REVERSE order (2024, 2023, ..., 2010)
years_to_process <- END_YEAR:START_YEAR

for (yr_idx in seq_along(years_to_process)) {
  yr <- years_to_process[yr_idx]
  dbg("=== PROCESSING YEAR %d (%d of %d) ===", yr, yr_idx, length(years_to_process))
  
  # ── STEP 1: Download this year's data ──────────────────────────────────────
  dbg("Step 1: Downloading year %d from Tapestry API...", yr)
  raw_df <- download_tapestry_year(yr)
  
  if (is.null(raw_df)) {
    dbg("  [SKIP] Year %d: download failed or returned NULL", yr)
    next
  }
  
  # ── STEP 2: Process this year ──────────────────────────────────────────────
  dbg("Step 2: Processing year %d...", yr)
  nv <- get_naics_version(yr)
  yr_data <- process_tapestry_year(yr, raw_df)
  
  # Free raw data immediately
  rm(raw_df)
  gc()
  Sys.sleep(0.5)
  
  if (is.null(yr_data)) {
    dbg("  [SKIP] Year %d: processing failed or returned NULL", yr)
    next
  }
  
  # ── STEP 3: Compute complexity for all geographic levels ───────────────────
  dbg("Step 3: Computing complexity for year %d...", yr)
  
  geo_levels <- list(
    county = yr_data$county,
    cbsa   = yr_data$cbsa,
    csa    = yr_data$csa,
    cz     = yr_data$cz
  )
  
  yr_results <- list()
  yr_diagnostics <- list()
  
  for (level_name in names(geo_levels)) {
    lq_df <- geo_levels[[level_name]]
    if (is.null(lq_df) || nrow(lq_df) == 0) next
    
    result <- tryCatch(
      compute_complexity(
        lq_df, paste(yr, level_name),
        create_industry_space = TRUE
      ),
      error = function(e) {
        dbg("  [ERROR] %s %s: %s", yr, level_name, e$message)
        NULL
      })
    
    if (!is.null(result)) {
      yr_results[[level_name]] <- result
      yr_diagnostics[[level_name]] <- result$diagnostics
      
      if (!is.null(result$M_matrix) && nrow(result$M_matrix) >= 5) {
        yr_results[[level_name]]$peers <- tryCatch(
          compute_peer_geographies(result$M_matrix),
          error = function(e) NULL)
      }
    }
  }
  
  # ── STEP 4: Build industry space network (LATEST YEAR, county-level) ───────
  if (yr == END_YEAR && !is.null(yr_results$county)) {
    dbg("Step 4: Building industry space network (latest year, county-level)...")
    county_result <- yr_results$county
    
    if (!is.null(county_result$proximity)) {
      phi <- county_result$proximity$proximity
      ind_codes_is <- colnames(phi)
      
      dbg("  Building industry space edges (top 5%% proximity + strongest per node)...")
      industry_space_edges <- create_edge_list(phi, ind_codes_is, top_pct = 0.05) %>%
        dplyr::left_join(
          industry_names_lookup %>%
            dplyr::rename(from = industry_code, from_desc = industry_description),
          by = "from"
        ) %>%
        dplyr::left_join(
          industry_names_lookup %>%
            dplyr::rename(to = industry_code, to_desc = industry_description),
          by = "to"
        )
      
      industry_space_nodes <- county_result$ici %>%
        dplyr::left_join(county_result$ubiquity, by = "industry_code") %>%
        dplyr::left_join(
          tibble::tibble(
            industry_code = names(county_result$proximity$centrality),
            centrality = county_result$proximity$centrality
          ),
          by = "industry_code") %>%
        dplyr::left_join(industry_names_lookup, by = "industry_code")
      
      dbg("  Industry space: %d edges, %d nodes",
          nrow(industry_space_edges), nrow(industry_space_nodes))
    }
  }
  
  # ── STEP 5: v2.4 State aggregation from county-level results ───────────────
  if (!is.null(yr_results$county)) {
    dbg("Step 5: Computing v2.4 state aggregation...")
    county_result <- yr_results$county
    county_lq_df <- yr_data$county
    
    county_emp <- county_lq_df %>%
      dplyr::group_by(geoid) %>%
      dplyr::summarise(total_emp = sum(employment, na.rm = TRUE), .groups = "drop")
    
    county_eci_with_emp <- county_result$eci %>%
      dplyr::left_join(county_emp, by = "geoid") %>%
      dplyr::left_join(
        county_crosswalk_df %>% dplyr::select(county_geoid, state_fips) %>%
          dplyr::rename(geoid = county_geoid),
        by = "geoid") %>%
      dplyr::filter(!is.na(state_fips), !is.na(total_emp))
    
    state_agg_eci <- county_eci_with_emp %>%
      dplyr::group_by(state_fips) %>%
      dplyr::summarise(
        economic_complexity_index = weighted.mean(economic_complexity_index, total_emp, na.rm = TRUE),
        n_counties = dplyr::n(),
        total_employment = sum(total_emp, na.rm = TRUE),
        .groups = "drop") %>%
      dplyr::rename(geoid = state_fips)
    
    state_ici <- county_result$ici
    
    # State diversity/ubiquity from native state-level M matrix
    state_lq_df <- yr_data$state
    state_native <- tryCatch(
      compute_complexity(
        state_lq_df, paste(yr, "state_native"),
        create_industry_space = FALSE, run_stress_tests = FALSE
      ),
      error = function(e) NULL)
    
    state_diversity <- if (!is.null(state_native)) state_native$diversity else NULL
    
    # ── v2.4: Aggregate county-level density/SG to state level ──
    # Per Lightcast methodology: for each state-industry pair, compute
    # employment-weighted mean of county-level density and strategic gain.
    state_proximity <- NULL
    state_M_agg <- NULL  # Reset from any prior year
    if (!is.null(county_result$proximity)) {
      dbg("  Aggregating county-level density/SG to state level...")
      county_density_mat <- county_result$proximity$density
      county_sg_mat      <- county_result$proximity$strategic_gain
      county_M_mat       <- county_result$M_matrix
      county_ici_vec     <- county_result$ici$industry_complexity_index
      names(county_ici_vec) <- county_result$ici$industry_code
      
      # Map counties to states with employment
      county_state_map <- county_eci_with_emp %>%
        dplyr::select(geoid, state_fips = state_fips, total_emp) %>%
        dplyr::filter(geoid %in% rownames(county_density_mat), total_emp > 0)
      # Fix: state_fips was renamed to geoid in state_agg_eci, re-derive from crosswalk
      county_state_map <- county_emp %>%
        dplyr::left_join(
          county_crosswalk_df %>% dplyr::select(county_geoid, state_fips) %>%
            dplyr::rename(geoid = county_geoid),
          by = "geoid") %>%
        dplyr::filter(!is.na(state_fips), total_emp > 0,
                      geoid %in% rownames(county_density_mat))
      
      state_density_list <- list()
      state_sg_list      <- list()
      state_M_agg_list   <- list()
      
      for (st in unique(county_state_map$state_fips)) {
        st_counties <- county_state_map %>% dplyr::filter(state_fips == st)
        valid_rows <- intersect(st_counties$geoid, rownames(county_density_mat))
        if (length(valid_rows) == 0) next
        
        emp_wts <- st_counties$total_emp[match(valid_rows, st_counties$geoid)]
        emp_wts_norm <- emp_wts / sum(emp_wts)
        
        # Emp-weighted avg density per industry
        density_sub <- county_density_mat[valid_rows, , drop = FALSE]
        state_density_list[[st]] <- as.vector(emp_wts_norm %*% density_sub)
        
        # Emp-weighted avg strategic gain per industry
        sg_sub <- county_sg_mat[valid_rows, , drop = FALSE]
        state_sg_list[[st]] <- as.vector(emp_wts_norm %*% sg_sub)
        
        # State M matrix: use state-level LQ for binary RCA
        if (!is.null(state_lq_df)) {
          state_lq_for_st <- state_lq_df %>%
            dplyr::filter(geoid == st) %>%
            dplyr::select(naics6_code, location_quotient)
          ind_codes_aligned <- colnames(county_density_mat)
          lq_vals <- state_lq_for_st$location_quotient[
            match(ind_codes_aligned, state_lq_for_st$naics6_code)]
          lq_vals[is.na(lq_vals)] <- 0
          state_M_agg_list[[st]] <- as.integer(lq_vals >= 1)
        }
      }
      
      if (length(state_density_list) > 0) {
        state_ids_agg    <- names(state_density_list)
        ind_codes_agg    <- colnames(county_density_mat)
        state_density_agg <- do.call(rbind, state_density_list)
        state_sg_agg      <- do.call(rbind, state_sg_list)
        state_M_agg       <- do.call(rbind, state_M_agg_list)
        
        rownames(state_density_agg) <- rownames(state_sg_agg) <- state_ids_agg
        colnames(state_density_agg) <- colnames(state_sg_agg) <- ind_codes_agg
        if (!is.null(state_M_agg)) {
          rownames(state_M_agg) <- state_ids_agg
          colnames(state_M_agg) <- ind_codes_agg
        }
        
        # Strategic index for aggregated states (using county ICI)
        ici_for_si <- county_ici_vec[ind_codes_agg]
        ici_for_si[is.na(ici_for_si)] <- 0
        state_si_agg <- rowSums(
          state_density_agg * (1 - state_M_agg) *
            matrix(ici_for_si, nrow = length(state_ids_agg),
                   ncol = length(ind_codes_agg), byrow = TRUE))
        names(state_si_agg) <- state_ids_agg
        
        state_proximity <- list(
          density = state_density_agg,
          strategic_gain = state_sg_agg,
          strategic_index = state_si_agg,
          centrality = county_result$proximity$centrality  # proxy from county
        )
        
        dbg("  State aggregated density: %d states x %d industries",
            nrow(state_density_agg), ncol(state_density_agg))
        dbg("  State density range: [%.4f, %.4f]",
            min(state_density_agg), max(state_density_agg))
        dbg("  State strategic index range: [%.2f, %.2f]",
            min(state_si_agg), max(state_si_agg))
      }
    }
    
    yr_results$state <- list(
      eci = state_agg_eci,
      ici = state_ici,
      diversity = state_diversity,
      ubiquity = county_result$ubiquity,
      M_matrix = if (exists("state_M_agg") && !is.null(state_M_agg)) state_M_agg else NULL,
      proximity = state_proximity,
      method = "v2.4_county_aggregation")
    
    yr_diagnostics$state <- list(
      method = "v2.4_county_aggregation",
      n_states = nrow(state_agg_eci),
      eci_range = range(state_agg_eci$economic_complexity_index),
      has_density = !is.null(state_proximity))
  }
  
  # ── STEP 6: Assembly & Export (year-specific data to chunked parquet files) ─
  dbg("Step 6: Assembling and exporting year %d data...", yr)
  
  for (level_name in names(yr_results)) {
    result <- yr_results[[level_name]]
    if (is.null(result) || is.null(result$eci)) next
    
    lq_df <- switch(level_name,
                    county = yr_data$county, state = yr_data$state,
                    cbsa = yr_data$cbsa, csa = yr_data$csa, cz = yr_data$cz)
    
    if (is.null(lq_df)) next
    
    # Geography-industry pairs (CHUNKED pivot for memory efficiency)
    gi_pairs <- lq_df %>%
      dplyr::select(geoid, naics6_code, employment, location_quotient) %>%
      dplyr::mutate(
        comparative_advantage = as.integer(location_quotient >= 1),
        year = yr,
        geo_level = level_name)
    
    gi_pairs <- gi_pairs %>%
      dplyr::left_join(result$eci, by = "geoid") %>%
      dplyr::left_join(result$ici %>% dplyr::rename(naics6_code = industry_code), by = "naics6_code")
    
    # Pivot density and strategic gain in BATCHES of 500 geos to avoid memory spikes
    if (!is.null(result$proximity)) {
      geo_ids_unique <- unique(gi_pairs$geoid)
      n_geos <- length(geo_ids_unique)
      batch_size <- 500
      
      density_list <- list()
      sg_list <- list()
      
      for (batch_start in seq(1, n_geos, by = batch_size)) {
        batch_end <- min(batch_start + batch_size - 1, n_geos)
        batch_geos <- geo_ids_unique[batch_start:batch_end]
        
        density_batch <- as.data.frame(
          result$proximity$density[match(batch_geos, rownames(result$proximity$density)), , drop = FALSE]
        ) %>%
          tibble::rownames_to_column("geoid") %>%
          tidyr::pivot_longer(-geoid, names_to = "naics6_code", values_to = "feasibility")
        density_list[[length(density_list) + 1]] <- density_batch
        
        sg_batch <- as.data.frame(
          result$proximity$strategic_gain[match(batch_geos, rownames(result$proximity$strategic_gain)), , drop = FALSE]
        ) %>%
          tibble::rownames_to_column("geoid") %>%
          tidyr::pivot_longer(-geoid, names_to = "naics6_code", values_to = "strategic_gain")
        sg_list[[length(sg_list) + 1]] <- sg_batch
      }
      
      density_df <- dplyr::bind_rows(density_list)
      sg_df <- dplyr::bind_rows(sg_list)
      
      gi_pairs <- gi_pairs %>%
        dplyr::left_join(density_df, by = c("geoid", "naics6_code")) %>%
        dplyr::left_join(sg_df, by = c("geoid", "naics6_code"))
      
      rm(density_list, sg_list, density_df, sg_df, geo_ids_unique)
      gc()
    }
    
    # Write gi_pairs chunk to disk immediately (never accumulate in memory)
    gi_chunk_count <- gi_chunk_count + 1
    chunk_file <- file.path(gi_chunk_dir,
                            sprintf("gi_%03d_%d_%s.parquet", gi_chunk_count, yr, level_name))
    arrow::write_parquet(gi_pairs, chunk_file)
    gi_total_rows <- gi_total_rows + nrow(gi_pairs)
    dbg("    [%s/%s] %s gi rows written to chunk %d",
        yr, level_name, format_number(nrow(gi_pairs)), gi_chunk_count)
    rm(gi_pairs); gc()
    
    # Geography metrics
    geo_metrics <- result$eci %>%
      dplyr::left_join(result$diversity, by = "geoid") %>%
      dplyr::mutate(year = yr, geo_level = level_name)
    
    if (!is.null(result$proximity)) {
      si_df <- tibble::tibble(
        geoid = names(result$proximity$strategic_index),
        strategic_index = result$proximity$strategic_index)
      geo_metrics <- geo_metrics %>% dplyr::left_join(si_df, by = "geoid")
    }
    
    geo_metrics <- geo_metrics %>%
      dplyr::mutate(
        eci_percentile = dplyr::percent_rank(economic_complexity_index) * 100)
    
    all_geo_metrics_list[[paste(yr, level_name, sep = "_")]] <- geo_metrics
    
    # Industry metrics
    ind_metrics <- result$ici %>%
      dplyr::left_join(result$ubiquity, by = "industry_code") %>%
      dplyr::mutate(
        year = yr, geo_level = level_name,
        ici_percentile = dplyr::percent_rank(industry_complexity_index) * 100)
    
    all_ind_metrics_list[[paste(yr, level_name, sep = "_")]] <- ind_metrics
    
    # Diagnostics
    diag_row <- as_tibble(result$diagnostics[sapply(result$diagnostics, function(x) length(x) == 1)]) %>%
      dplyr::mutate(year = yr, geo_level = level_name)
    all_diagnostics_list[[paste(yr, level_name, sep = "_")]] <- diag_row
    
    # Peer geographies
    if (!is.null(result$peers)) {
      peers_with_meta <- result$peers %>%
        dplyr::mutate(year = yr, geo_level = level_name)
      all_peer_data_list[[paste(yr, level_name, sep = "_")]] <- peers_with_meta
    }
  }
  
  # ── STEP 7: Per-year hierarchical export & GitHub upload ───────────────────
  dbg("Step 7: Exporting & uploading year %d (hierarchical directory)...", yr)
  
  yr_upload_ok <- 0
  yr_upload_fail <- 0
  yr_files_created <- 0
  
  # Gather this year's data from accumulators
  yr_geo_keys <- grep(paste0("^", yr, "_"), names(all_geo_metrics_list), value = TRUE)
  yr_ind_keys <- grep(paste0("^", yr, "_"), names(all_ind_metrics_list), value = TRUE)
  yr_diag_keys <- grep(paste0("^", yr, "_"), names(all_diagnostics_list), value = TRUE)
  
  yr_geo_df <- dplyr::bind_rows(all_geo_metrics_list[yr_geo_keys])
  yr_ind_df <- dplyr::bind_rows(all_ind_metrics_list[yr_ind_keys])
  yr_diag_df <- dplyr::bind_rows(all_diagnostics_list[yr_diag_keys])
  
  # Combine gi_pairs chunks for this year
  yr_gi_chunks <- sort(list.files(gi_chunk_dir,
                                  pattern = sprintf("gi_\\d+_%d_", yr), full.names = TRUE))
  yr_gi_combined <- NULL
  if (length(yr_gi_chunks) > 0) {
    yr_gi_parts <- lapply(yr_gi_chunks, arrow::read_parquet)
    yr_gi_combined <- dplyr::bind_rows(yr_gi_parts)
    rm(yr_gi_parts); gc(verbose = FALSE)
    dbg("  gi_pairs for %d: %s rows from %d chunks",
        yr, format_number(nrow(yr_gi_combined)), length(yr_gi_chunks))
  }
  
  # ── Build CONSOLIDATED hierarchical directory structure ──
  # {year}/
  #   industry_geography_data/
  #     by_industry/all_industries.csv.gz         (single file, all NAICS codes)
  #     by_geography/county/{State_Name}_county_level.csv.gz  (one per state)
  #     by_geography/state/all_states.csv.gz
  #     by_geography/core_based_statistical_area/all_cbsa.csv.gz
  #     by_geography/combined_statistical_area/all_csa.csv.gz
  #     by_geography/commuting_zone/all_cz.csv.gz
  #   industry_specific_data/
  #     all_industries.csv.gz                     (single file)
  #   geography_specific_data/
  #     by_geography/county/{State_Name}_county_level.csv.gz
  #     by_geography/state/all_states.csv.gz
  #     by_geography/{other_level}/all_{level}.csv.gz
  #   diagnostics.csv.gz
  
  yr_base <- file.path(output_folder, as.character(yr))
  ig_dir <- file.path(yr_base, "industry_geography_data")
  ig_by_ind <- file.path(ig_dir, "by_industry")
  ig_by_geo <- file.path(ig_dir, "by_geography")
  is_dir <- file.path(yr_base, "industry_specific_data")
  gs_dir <- file.path(yr_base, "geography_specific_data", "by_geography")
  
  for (d in c(ig_by_ind, ig_by_geo, is_dir, gs_dir)) {
    dir.create(d, showWarnings = FALSE, recursive = TRUE)
  }
  
  # ── State name lookup for consolidated filenames ──
  state_fips_to_name <- c(
    "01" = "Alabama", "02" = "Alaska", "04" = "Arizona", "05" = "Arkansas",
    "06" = "California", "08" = "Colorado", "09" = "Connecticut", "10" = "Delaware",
    "11" = "District_of_Columbia", "12" = "Florida", "13" = "Georgia",
    "15" = "Hawaii", "16" = "Idaho", "17" = "Illinois", "18" = "Indiana",
    "19" = "Iowa", "20" = "Kansas", "21" = "Kentucky", "22" = "Louisiana",
    "23" = "Maine", "24" = "Maryland", "25" = "Massachusetts", "26" = "Michigan",
    "27" = "Minnesota", "28" = "Mississippi", "29" = "Missouri", "30" = "Montana",
    "31" = "Nebraska", "32" = "Nevada", "33" = "New_Hampshire",
    "34" = "New_Jersey", "35" = "New_Mexico", "36" = "New_York",
    "37" = "North_Carolina", "38" = "North_Dakota", "39" = "Ohio",
    "40" = "Oklahoma", "41" = "Oregon", "42" = "Pennsylvania",
    "44" = "Rhode_Island", "45" = "South_Carolina", "46" = "South_Dakota",
    "47" = "Tennessee", "48" = "Texas", "49" = "Utah", "50" = "Vermont",
    "51" = "Virginia", "53" = "Washington", "54" = "West_Virginia",
    "55" = "Wisconsin", "56" = "Wyoming", "60" = "American_Samoa",
    "66" = "Guam", "69" = "Northern_Mariana_Islands", "72" = "Puerto_Rico",
    "78" = "US_Virgin_Islands")
  
  # Helper: geo level → directory name and file prefix
  level_dir_and_prefix <- function(lvl) {
    switch(lvl,
           county = list(dir = "county",                          prefix = "county"),
           state  = list(dir = "state",                           prefix = "state"),
           cbsa   = list(dir = "core_based_statistical_area",     prefix = "cbsa"),
           csa    = list(dir = "combined_statistical_area",       prefix = "csa"),
           cz     = list(dir = "commuting_zone",                  prefix = "cz"))
  }
  
  # ── Write industry_geography_data (consolidated) ──
  if (!is.null(yr_gi_combined) && nrow(yr_gi_combined) > 0) {
    
    # by_industry: single file with all NAICS codes
    dbg("  Writing industry_geography_data/by_industry/all_industries.csv.gz ...")
    write_csv_gz(yr_gi_combined, file.path(ig_by_ind, "all_industries.csv.gz"))
    yr_files_created <- yr_files_created + 1
    
    # by_geography: consolidated by state (county level) or single file (other levels)
    dbg("  Writing industry_geography_data/by_geography/ (consolidated) ...")
    for (lvl in unique(yr_gi_combined$geo_level)) {
      info <- level_dir_and_prefix(lvl)
      lvl_dir <- file.path(ig_by_geo, info$dir)
      dir.create(lvl_dir, showWarnings = FALSE, recursive = TRUE)
      lvl_data <- yr_gi_combined %>% dplyr::filter(geo_level == lvl)
      
      if (lvl == "county") {
        # One file per state: {State_Name}_county_level.csv.gz
        lvl_data <- lvl_data %>% dplyr::mutate(
          .state_fips = substr(geoid, 1, 2),
          .state_name = state_fips_to_name[.state_fips])
        for (st in unique(lvl_data$.state_fips)) {
          st_data <- lvl_data %>% dplyr::filter(.state_fips == st) %>%
            dplyr::select(-`.state_fips`, -`.state_name`)
          st_name <- state_fips_to_name[st] %||% paste0("State_", st)
          fname <- sprintf("%s_county_level.csv.gz", st_name)
          write_csv_gz(st_data, file.path(lvl_dir, fname))
          yr_files_created <- yr_files_created + 1
        }
        dbg("    county: %d state files", length(unique(lvl_data$.state_fips)))
      } else {
        # Single file for state/CBSA/CSA/CZ
        fname <- sprintf("all_%s.csv.gz", info$prefix)
        write_csv_gz(lvl_data, file.path(lvl_dir, fname))
        yr_files_created <- yr_files_created + 1
        dbg("    %s: 1 consolidated file (%s rows)",
            info$dir, format_number(nrow(lvl_data)))
      }
    }
  }
  
  # ── Write industry_specific_data (single consolidated file) ──
  if (nrow(yr_ind_df) > 0) {
    dbg("  Writing industry_specific_data/all_industries.csv.gz ...")
    write_csv_gz(yr_ind_df, file.path(is_dir, "all_industries.csv.gz"))
    yr_files_created <- yr_files_created + 1
    dbg("    %s industry rows written", format_number(nrow(yr_ind_df)))
  }
  
  # ── Write geography_specific_data (consolidated: by state for county, single file for others) ──
  if (nrow(yr_geo_df) > 0) {
    dbg("  Writing geography_specific_data/by_geography/ (consolidated) ...")
    for (lvl in unique(yr_geo_df$geo_level)) {
      info <- level_dir_and_prefix(lvl)
      lvl_dir <- file.path(gs_dir, info$dir)
      dir.create(lvl_dir, showWarnings = FALSE, recursive = TRUE)
      lvl_data <- yr_geo_df %>% dplyr::filter(geo_level == lvl)
      
      if (lvl == "county") {
        lvl_data <- lvl_data %>% dplyr::mutate(
          .state_fips = substr(geoid, 1, 2),
          .state_name = state_fips_to_name[.state_fips])
        for (st in unique(lvl_data$.state_fips)) {
          st_data <- lvl_data %>% dplyr::filter(.state_fips == st) %>%
            dplyr::select(-`.state_fips`, -`.state_name`)
          st_name <- state_fips_to_name[st] %||% paste0("State_", st)
          fname <- sprintf("%s_county_level.csv.gz", st_name)
          write_csv_gz(st_data, file.path(lvl_dir, fname))
          yr_files_created <- yr_files_created + 1
        }
        dbg("    county: %d state files", length(unique(lvl_data$.state_fips)))
      } else {
        fname <- sprintf("all_%s.csv.gz", info$prefix)
        write_csv_gz(lvl_data, file.path(lvl_dir, fname))
        yr_files_created <- yr_files_created + 1
        dbg("    %s: 1 consolidated file (%s rows)",
            info$dir, format_number(nrow(lvl_data)))
      }
    }
  }
  
  # ── Write diagnostics at year level ──
  if (nrow(yr_diag_df) > 0) {
    write_csv_gz(yr_diag_df, file.path(yr_base, "diagnostics.csv.gz"))
    yr_files_created <- yr_files_created + 1
  }
  
  dbg("  Year %d: %s files created in hierarchical structure",
      yr, format_number(yr_files_created))
  
  rm(yr_geo_df, yr_ind_df, yr_diag_df, yr_gi_combined); gc(verbose = FALSE)
  
  # ── Upload to GitHub (hierarchical) ──
  if (github_ready) {
    # Recursively collect all files in the year directory
    yr_all_files <- list.files(yr_base, recursive = TRUE, full.names = TRUE)
    gh_yr_base <- sprintf("%s/%d", GITHUB_FOLDER, yr)
    commit_msg <- sprintf("Add year %d economic complexity data", yr)
    
    # Check sizes — files > 50MB need GitHub Release/LFS
    size_limit <- 50 * 1024^2
    normal_files <- yr_all_files[file.size(yr_all_files) <= size_limit]
    large_files <- yr_all_files[file.size(yr_all_files) > size_limit]
    
    dbg("  Uploading %d files to %s/%s/ (skipping %d files > 50MB)...",
        length(normal_files), GITHUB_REPO, gh_yr_base, length(large_files))
    
    # Upload in batches to avoid GitHub API rate limits
    for (lf in normal_files) {
      rel_path <- sub(paste0(yr_base, "/"), "", lf)
      gh_path <- paste0(gh_yr_base, "/", rel_path)
      ok <- upload_file_to_github(lf, gh_path, commit_msg = commit_msg)
      if (ok) yr_upload_ok <- yr_upload_ok + 1 else yr_upload_fail <- yr_upload_fail + 1
    }
    
    # Handle large files via GitHub Release
    if (length(large_files) > 0) {
      dbg("  [LFS] %d files exceed 50MB — uploading via GitHub Release...", length(large_files))
      tag_name <- paste0("v", yr, "_", timestamp_str)
      release_body <- list(
        tag_name = tag_name,
        name = sprintf("Large files: Year %d", yr),
        body = sprintf("Large output files for year %d (> 50MB)", yr),
        draft = FALSE, prerelease = FALSE)
      
      release_resp <- tryCatch(
        httr::POST(
          paste0(github_api_base, "/releases"),
          body = jsonlite::toJSON(release_body, auto_unbox = TRUE),
          gh_headers()),
        error = function(e) NULL)
      
      if (!is.null(release_resp) && httr::status_code(release_resp) %in% c(200, 201)) {
        upload_url_base <- gsub("\\{.*\\}", "",
                                httr::content(release_resp)$upload_url)
        for (lf in large_files) {
          upload_url <- paste0(upload_url_base,
                               "?name=", utils::URLencode(basename(lf)))
          asset_resp <- httr::POST(upload_url,
                                   body = httr::upload_file(lf),
                                   httr::add_headers(
                                     Authorization = paste("token", github_token),
                                     `Content-Type` = "application/octet-stream"),
                                   httr::timeout(600))
          if (httr::status_code(asset_resp) %in% c(200, 201)) {
            yr_upload_ok <- yr_upload_ok + 1
            dbg("    [RELEASE] %s (%s)", basename(lf), format_bytes(file.size(lf)))
          } else {
            yr_upload_fail <- yr_upload_fail + 1
            dbg("    [FAIL] Release asset %s: HTTP %d",
                basename(lf), httr::status_code(asset_resp))
          }
        }
      } else {
        dbg("  [FAIL] Could not create release for large files")
        yr_upload_fail <- yr_upload_fail + length(large_files)
      }
    }
    
    # ── Verify a SAMPLE of uploads (don't verify every single CSV) ──
    dbg("  Verifying sample of uploads for year %d...", yr)
    verify_ok <- 0
    verify_fail <- 0
    # Verify diagnostics + a sample of files from key subdirectories
    sample_files <- c(
      file.path(yr_base, "diagnostics.csv.gz"),
      file.path(ig_by_ind, "all_industries.csv.gz"),
      file.path(is_dir, "all_industries.csv.gz"),
      head(list.files(file.path(ig_by_geo), pattern = "\\.csv\\.gz$",
                      full.names = TRUE, recursive = TRUE), 3)
    )
    sample_files <- sample_files[file.exists(sample_files)]
    
    for (lf in sample_files) {
      rel_path <- sub(paste0(yr_base, "/"), "", lf)
      gh_path <- paste0(gh_yr_base, "/", rel_path)
      vf <- verify_github_file(gh_path)
      if (vf$exists) {
        verify_ok <- verify_ok + 1
        dbg("    [VERIFIED] %s", rel_path)
      } else {
        verify_fail <- verify_fail + 1
        dbg("    [MISSING!] %s — NOT found on GitHub!", rel_path)
      }
    }
    
    # ── Show top-level folder structure ──
    dbg("  GitHub folder structure after year %d:", yr)
    show_github_folder_structure(gh_yr_base)
    
    # ── Summary for this year ──
    total_yr_files <- length(normal_files) + length(large_files)
    yr_status <- if (verify_fail == 0 && yr_upload_fail == 0) "SUCCESS" else "PARTIAL FAILURE"
    cat(sprintf(
      "\n    ╔══════════════════════════════════════════════════════════╗\n"))
    cat(sprintf(
      "    ║  YEAR %d UPLOAD: %-40s  ║\n", yr, yr_status))
    cat(sprintf(
      "    ║  Uploaded: %d/%d | Verified: %d sample | Failed: %d%s║\n",
      yr_upload_ok, total_yr_files, verify_ok, yr_upload_fail,
      strrep(" ", max(0, 10 - nchar(sprintf("%d", yr_upload_fail))))))
    cat(sprintf(
      "    ╚══════════════════════════════════════════════════════════╝\n\n"))
    
    if (verify_fail > 0) {
      dbg("  [WARN] %d sampled files failed verification for year %d!", verify_fail, yr)
      dbg("  Pipeline will CONTINUE but check GitHub manually for missing files.")
    }
    
    year_upload_log[[as.character(yr)]] <- list(
      year = yr,
      uploaded = yr_upload_ok,
      failed = yr_upload_fail,
      verified = verify_ok,
      verify_failed = verify_fail,
      total_files = total_yr_files,
      status = yr_status)
  } else {
    dbg("  [SKIP] GitHub upload disabled (no token). Files saved locally: %s", yr_base)
    year_upload_log[[as.character(yr)]] <- list(
      year = yr, uploaded = 0, failed = 0, verified = 0,
      verify_failed = 0, total_files = yr_files_created, status = "SKIPPED (no token)")
  }
  
  # ── STEP 8: Free this year's data ──────────────────────────────────────────
  dbg("Step 8: Freeing year %d data from memory...", yr)
  rm(yr_data, yr_results, yr_diagnostics, geo_levels)
  gc()
  Sys.sleep(0.5)
  
  dbg("  Year %d complete. Memory freed.\n", yr)
}

dbg("All years processed successfully")
end_section("SECTION 8: Main Loop — Year-by-Year Processing")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9: TOP-LEVEL CROSSWALKS, INDUSTRY SPACE & CLEANUP
# ══════════════════════════════════════════════════════════════════════════════

start_section("SECTION 9: Top-Level Crosswalks, Industry Space & Cleanup")

dbg("Exporting top-level crosswalks and reference files...")

# ── Export crosswalks and hierarchy at top level of output folder ──
crosswalk_dir <- file.path(output_folder, "crosswalks")
dir.create(crosswalk_dir, showWarnings = FALSE, recursive = TRUE)

write_csv_gz(county_crosswalk_df, file.path(crosswalk_dir, "county_crosswalk.csv.gz"))
dbg("  county_crosswalk.csv.gz: %s rows", format_number(nrow(county_crosswalk_df)))

# Export NAICS hierarchy
if (exists("naics_title_lookups") && length(naics_title_lookups) > 0) {
  for (version_name in names(naics_title_lookups)) {
    hierarchy_path <- file.path(crosswalk_dir,
                                sprintf("naics_hierarchy_%s.csv.gz", version_name))
    write_csv_gz(naics_title_lookups[[version_name]], hierarchy_path)
  }
  dbg("  NAICS hierarchy files: %d versions", length(naics_title_lookups))
}

# ── Export industry space (latest year, county-level) ──
industry_space_dir <- file.path(output_folder, "industry_space")
dir.create(industry_space_dir, showWarnings = FALSE, recursive = TRUE)

if (!is.null(industry_space_edges)) {
  write_csv_gz(industry_space_edges,
               file.path(industry_space_dir, "industry_space_edges.csv.gz"))
  dbg("  industry_space_edges.csv.gz: %s rows", format_number(nrow(industry_space_edges)))
}
if (!is.null(industry_space_nodes)) {
  write_csv_gz(industry_space_nodes,
               file.path(industry_space_dir, "industry_space_nodes.csv.gz"))
  dbg("  industry_space_nodes.csv.gz: %s rows", format_number(nrow(industry_space_nodes)))
}

# ── Export peer geographies ──
if (length(all_peer_data_list) > 0) {
  peer_df <- dplyr::bind_rows(all_peer_data_list)
  peer_dir <- file.path(output_folder, "peer_geographies")
  dir.create(peer_dir, showWarnings = FALSE, recursive = TRUE)
  write_csv_gz(peer_df, file.path(peer_dir, "peer_geographies.csv.gz"))
  dbg("  peer_geographies.csv.gz: %s rows", format_number(nrow(peer_df)))
}

# ── Clean up gi_pairs chunk directory (individual year CSVs are the primary output now) ──
unlink(gi_chunk_dir, recursive = TRUE)

# Free accumulators
rm(all_geo_metrics_list, all_ind_metrics_list, all_diagnostics_list, all_peer_data_list)
gc(verbose = FALSE)

dbg("  Total gi_pairs rows across all years: %s", format_number(gi_total_rows))

end_section("SECTION 9: Top-Level Crosswalks, Industry Space & Cleanup")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 10: README GENERATION
# ══════════════════════════════════════════════════════════════════════════════

start_section("SECTION 10: README Generation")

readme_lines <- c(
  sprintf("# %s", GITHUB_FOLDER),
  "",
  "## Overview",
  "",
  "Economic complexity analysis of U.S. employment data from the Tapestry QCEW database,",
  sprintf("computed for years %d through %d using the Daboin et al. (2019) eigenvector", START_YEAR, END_YEAR),
  "methodology. All metrics (ECI, ICI, proximity, density, strategic gain) are computed",
  "at five geographic levels: county, state, CBSA, CSA, and commuting zone.",
  "",
  sprintf("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  "## Data Sources",
  "",
  "| Source | Description | URL |",
  "|--------|-------------|-----|",
  sprintf("| Tapestry QCEW | Employment data (Rule %d) | https://tapestry.nkn.uidaho.edu |", WAGE_RULE),
  "| BLS | NAICS hierarchy crosswalk | https://www.bls.gov/cew/classifications/industry/ |",
  "| Census LODES | Block-level employment for CT/AK harmonization | https://lehd.ces.census.gov |",
  "| TIGRIS 2024 | County, CBSA, CSA boundaries | via R `tigris` package |",
  "| USDA ERS | 2020 Commuting Zone definitions | https://www.ers.usda.gov |",
  "",
  "## Methodology",
  "",
  "All formulas follow Daboin et al. (2019), *Economic Complexity and Technological",
  "Relatedness: Findings for American Cities*. No minimum ubiquity filtering is applied;",
  "the M matrix is M[c,i] = 1{RCA >= 1} directly per equations 4-5.",
  "",
  "### Revealed Comparative Advantage (RCA) — Eq. 4",
  "```",
  "LQ[c,i] = (emp[c,i] / emp[c]) / (emp[nation,i] / emp[nation])",
  "M[c,i]  = 1 if LQ[c,i] >= 1, else 0    (Eq. 5)",
  "```",
  "",
  "### ECI/ICI — Eqs. 12-14",
  "```",
  "M_tilde_C[c,c'] = sum_i M[c,i]*M[c',i] / (diversity[c] * ubiquity[i])",
  "ECI = 2nd eigenvector of M_tilde_C",
  "ICI = 2nd eigenvector of M_tilde_I (dual)",
  "```",
  "Sign correction: ICI negatively correlated with ubiquity; ECI positively",
  "correlated with average ICI of specialized industries.",
  "",
  "### Proximity — Eq. 16",
  "```",
  "phi[i,j] = U[i,j] / max(ubiquity[i], ubiquity[j])",
  "```",
  "",
  "### Density (Feasibility) — Eq. 19",
  "```",
  "density[c,i] = sum_j M[c,j]*phi[j,i] / sum_j phi[j,i]",
  "```",
  "",
  "### Centrality — Eq. 17",
  "```",
  "centrality[i] = sum_j phi[i,j] / sum_all phi",
  "```",
  "",
  "### Strategic Index — Eq. 21",
  "```",
  "SI[c] = sum_i density[c,i] * (1-M[c,i]) * ICI[i]",
  "```",
  "",
  "### Strategic Gain — Eq. 22",
  "```",
  "SG[c,i] = sum_j (phi_norm[i,j]*(1-M[c,j])*ICI[j]) - density[c,i]*ICI[i]",
  "```",
  "",
  "### State-Level Aggregation (v2.4)",
  "",
  "State ECI = employment-weighted mean of county ECI values.",
  "State density/strategic gain = employment-weighted mean of county-level matrices.",
  "State M = binary RCA from state-level LQ.",
  "",
  "## Geographic Harmonization",
  "",
  "### Connecticut (2010-2023)",
  "Eight old counties -> nine planning regions via LODES employment-weighted allocation.",
  "",
  "### Alaska (2010-2019)",
  "Valdez-Cordova (02261) -> Chugach (02063) + Copper River (02066) via LODES.",
  "",
  "### Other GEOID Corrections",
  "- Wade Hampton (02270) -> Kusilvak (02158)",
  "- Shannon County (46113) -> Oglala Lakota (46102)",
  "- Bedford City (51515) -> Bedford County (51019)",
  "",
  "## NAICS Code Aggregation",
  "",
  "- BLS specialty trade contractor codes: 238XX1/238XX2 collapsed to 238XX0",
  "- Three-digit rollups: 812XXX->812000, 111XXX->111000, 112XXX->112000, 51319X->513190",
  "",
  "## Directory Structure",
  "",
  "All data files use gzipped CSV (.csv.gz) for efficient storage and transfer.",
  "County-level data is consolidated into one file per state; other geographic",
  "levels use a single file per level.",
  "",
  "```",
  sprintf("%s/", GITHUB_FOLDER),
  "  crosswalks/",
  "    county_crosswalk.csv.gz",
  "    naics_hierarchy_v2022.csv.gz  (etc.)",
  "  industry_space/",
  "    industry_space_edges.csv.gz",
  "    industry_space_nodes.csv.gz",
  "  peer_geographies/",
  "    peer_geographies.csv.gz",
  "  {year}/                              (e.g., 2024/)",
  "    diagnostics.csv.gz",
  "    industry_geography_data/",
  "      by_industry/",
  "        all_industries.csv.gz          (all NAICS codes in one file)",
  "      by_geography/",
  "        county/{State_Name}_county_level.csv.gz   (one per state)",
  "        state/all_state.csv.gz",
  "        core_based_statistical_area/all_cbsa.csv.gz",
  "        combined_statistical_area/all_csa.csv.gz",
  "        commuting_zone/all_cz.csv.gz",
  "    industry_specific_data/",
  "      all_industries.csv.gz            (ICI, ubiquity, percentiles)",
  "    geography_specific_data/",
  "      by_geography/",
  "        county/{State_Name}_county_level.csv.gz",
  "        state/all_state.csv.gz",
  "        (same pattern as above)",
  "  README.md",
  sprintf("  %s", "Tapestry_Economic_Complexity_2010_2024.R"),
  "```",
  "",
  "## Geographic Levels",
  "",
  "| Level | Description | Complexity Method |",
  "|-------|-------------|-------------------|",
  "| County | ~3,100 US counties (2024 TIGRIS) | Native eigenvector |",
  "| State | 50 states + DC | v2.4 county aggregation (density/SG emp-weighted) |",
  "| CBSA | Core Based Statistical Areas (2024) | Native eigenvector |",
  "| CSA | Combined Statistical Areas (2024) | Native eigenvector |",
  "| CZ | 2020 Commuting Zones (USDA ERS) | Native eigenvector |",
  "",
  "## References",
  "",
  "- Daboin, C., Johnson, S., Kotsadam, A., Neffke, F., & O'Clery, N. (2019).",
  "  *Economic Complexity and Technological Relatedness: Findings for American Cities.*",
  "- Hausmann, R. & Hidalgo, C. (2009). *The building blocks of economic complexity.*",
  "  PNAS, 106(26), 10570-10575.",
  "- Escobari, M., Seyal, I., Morales-Arilla, J., & Shearer, C.",
  "  *Growing Cities that Work for All.* Brookings Institution."
)

readme_content <- paste(readme_lines, collapse = "\n")
readme_path <- file.path(output_folder, "README.md")
writeLines(readme_content, readme_path)
dbg("README.md written (%s)", format_bytes(file.size(readme_path)))

# Copy script source to output folder
script_source <- tryCatch(sys.frame(1)$ofile, error = function(e) NULL)
if (is.null(script_source)) {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- cmd_args[grep("--file=", cmd_args)]
  if (length(file_arg) > 0)
    script_source <- tryCatch(normalizePath(sub("--file=", "", file_arg[1])),
                              error = function(e) NULL)
}
if (is.null(script_source) || !file.exists(script_source)) {
  candidates <- list.files(pattern = "Tapestry_Economic_Complexity.*\\.R$", full.names = TRUE)
  if (length(candidates) > 0) script_source <- candidates[1]
}
if (!is.null(script_source) && file.exists(script_source)) {
  file.copy(script_source, file.path(output_folder, basename(script_source)), overwrite = TRUE)
  dbg("Script copied to output folder")
} else {
  dbg("  [WARN] Could not locate script source for copying")
}

end_section("SECTION 10: README Generation")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 11: FINAL GITHUB UPLOAD (top-level files: crosswalks, README, script)
# ══════════════════════════════════════════════════════════════════════════════

start_section("SECTION 11: Final GitHub Upload (top-level files)")

if (!github_ready) {
  dbg("[WARN] No GitHub token. Files saved locally: %s", output_folder)
  dbg("  Set GITHUB_PAT and re-run, or upload manually.")
} else {
  
  # Upload top-level files: crosswalks, industry space, peer geographies, README, script
  top_level_dirs <- c("crosswalks", "industry_space", "peer_geographies")
  top_level_files <- c()
  
  for (tld in top_level_dirs) {
    tld_path <- file.path(output_folder, tld)
    if (dir.exists(tld_path)) {
      tld_files <- list.files(tld_path, full.names = TRUE, recursive = TRUE)
      top_level_files <- c(top_level_files, tld_files)
    }
  }
  
  # Add README and script
  readme_file <- file.path(output_folder, "README.md")
  if (file.exists(readme_file)) top_level_files <- c(top_level_files, readme_file)
  script_file <- list.files(output_folder, pattern = "\\.R$", full.names = TRUE)
  top_level_files <- c(top_level_files, script_file)
  
  dbg("Uploading %d top-level files to %s/%s/...",
      length(top_level_files), GITHUB_REPO, GITHUB_FOLDER)
  
  upload_success <- 0
  for (fp in top_level_files) {
    rel_path <- sub(paste0(output_folder, "/"), "", fp)
    gh_path <- paste0(GITHUB_FOLDER, "/", rel_path)
    ok <- upload_file_to_github(fp, gh_path,
                                commit_msg = sprintf("Add %s", rel_path))
    if (ok) upload_success <- upload_success + 1
  }
  
  dbg("Uploaded %d of %d top-level files", upload_success, length(top_level_files))
  
  # ── Verify top-level uploads ──
  dbg("Verifying top-level uploads...")
  for (fp in top_level_files) {
    rel_path <- sub(paste0(output_folder, "/"), "", fp)
    gh_path <- paste0(GITHUB_FOLDER, "/", rel_path)
    vf <- verify_github_file(gh_path)
    if (vf$exists) {
      dbg("  [VERIFIED] %s", rel_path)
    } else {
      dbg("  [MISSING!] %s", rel_path)
    }
  }
  
  # ── Show top-level GitHub folder structure ──
  cat("\n")
  cat(strrep("─", 70), "\n")
  cat("FINAL GITHUB FOLDER STRUCTURE\n")
  cat(strrep("─", 70), "\n")
  show_github_folder_structure(GITHUB_FOLDER)
  cat(strrep("─", 70), "\n")
  
  # ── Per-year upload summary table ──
  cat("\n")
  cat(strrep("═", 70), "\n")
  cat("PER-YEAR UPLOAD SUMMARY\n")
  cat(strrep("═", 70), "\n")
  cat(sprintf("  %-6s  %-12s  %-12s  %-10s  %-20s\n",
              "Year", "Uploaded", "Verified", "Failed", "Status"))
  cat(sprintf("  %-6s  %-12s  %-12s  %-10s  %-20s\n",
              "----", "--------", "--------", "------", "------"))
  for (yr_char in names(year_upload_log)) {
    log_entry <- year_upload_log[[yr_char]]
    cat(sprintf("  %-6s  %-12s  %-12s  %-10s  %-20s\n",
                log_entry$year,
                sprintf("%d files", log_entry$uploaded),
                sprintf("%d sampled", log_entry$verified),
                sprintf("%d", log_entry$verify_failed + log_entry$failed),
                log_entry$status))
  }
  total_uploaded <- sum(sapply(year_upload_log, function(x) x$uploaded))
  total_failed <- sum(sapply(year_upload_log, function(x) x$verify_failed + x$failed))
  cat(sprintf("  %-6s  %-12s  %-12s  %-10s  %-20s\n",
              "TOTAL",
              sprintf("%d files", total_uploaded + upload_success),
              "", sprintf("%d", total_failed),
              if (total_failed == 0) "ALL SUCCESS" else "HAS FAILURES"))
  cat(strrep("═", 70), "\n\n")
}

end_section("SECTION 11: Final GitHub Upload")

# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE COMPLETE
# ══════════════════════════════════════════════════════════════════════════════

total_elapsed <- as.numeric(difftime(Sys.time(), pipeline_start_time, units = "mins"))

cat("\n")
cat(strrep("=", 80), "\n")
cat("PIPELINE COMPLETE\n")
cat(sprintf("  Total time: %.1f minutes\n", total_elapsed))
cat(sprintf("  Years processed: %d-%d (%d years)\n",
            START_YEAR, END_YEAR, length(years_to_process)))
cat(sprintf("  Output folder: %s\n", output_folder))
cat(sprintf("  GitHub folder: %s/%s\n", GITHUB_REPO, GITHUB_FOLDER))
cat(sprintf("  Geography-industry pairs: %s total rows\n",
            format_number(gi_total_rows)))
cat("  Geographic levels: county, state (v2.4), CBSA, CSA, CZ\n")
cat("  All levels include: ECI, ICI, proximity, density, strategic gain\n")
cat(strrep("=", 80), "\n")