# =============================================================================
# LIGHTCAST EMPLOYMENT ANALYSIS SCRIPT WITH COMPLEXITY CALCULATIONS
# DABOIN METHOD - FULLY VALIDATED WITH EXHAUSTIVE STRESS TESTING
# VERSION 2.1 - WITH PERFORMANCE OPTIMIZATIONS AND PEER GEOGRAPHY ANALYSIS
# Cross-platform compatible (Mac/Windows)
# =============================================================================
#
# This script implements the full economic complexity methodology from:
# "Economic Complexity and Technological Relatedness: Findings for American Cities"
# by Daboin et al.
#
# VERSION 2.1 UPDATES:
# - Streamlined console output with verbosity control (VERBOSITY = 0/1/2/3)
# - Removed experimental alternative state methods (adhering strictly to Daboin)
# - Vectorized edge list creation for industry space
# - Parallel processing for strategic gain calculation
# - Peer geography analysis using Jaccard similarity
#
# REFERENCE EQUATIONS (from Daboin paper):
# =========================================
#
# SECTION 2.1: Economic Complexity Index
# --------------------------------------
# Eq. 1:  X_c = Σ_i J_ci                    (Total employment by city)
# Eq. 2:  X_i = Σ_c J_ci                    (Total employment by industry)
# Eq. 3:  X = ΣΣ J_ci                       (Total national employment)
# Eq. 4:  RCA_ci = (X_ci/X_c) / (X_i/X)     (Revealed Comparative Advantage)
# Eq. 5:  M_ci = 1[RCA_ci ≥ 1]              (Binary specialization matrix)
# Eq. 6:  Diversity_c = K_c0 = Σ_i M_ci    (Diversity of city c)
# Eq. 7:  Ubiquity_i = K_i0 = Σ_c M_ci     (Ubiquity of industry i)
# Eq. 8:  K_c1 = (Σ_i K_i0 * M_ci) / K_c0 → ... → K_c∞ = ECI_c  (Method of Reflections - City)
# Eq. 9:  K_i1 = (Σ_c K_c0 * M_ci) / K_i0 → ... → K_i∞ = ICI_i  (Method of Reflections - Industry)
# Eq. 10: K_c,n = (1/k_c,0) M_ci (1/k_i,0) Σ_c' M_c'i k_c',n-2
# Eq. 11: K_c,n = Σ_c' k_c',n-2 Σ_i (M_c'i k_c',n-2) / (k_c,0 k_i,0)
# Eq. 12: K_c,n = Σ_c' k_c',n-2 M̃^C_c,c'
#         where M̃^C_c,c' = Σ_i (M_c'i k_c',n-2) / (k_c,0 k_i,0)
# Eq. 13: k_n = M̃^C * k_{n-2}              (Vector notation)
# Eq. 14: M̃^C * k_{n-2} = λk as n → ∞      (Eigenvalue problem)
#
# KEY RESULT: ECI = 2nd eigenvector of M̃^C; ICI = 2nd eigenvector of M̃^I
#
# SECTION 2.2: Proximity and Density
# ----------------------------------
# Eq. 15: co-occurrence_{i,i'} = U_{i,i'} = M^T_ci * M_ci  (Co-occurrence matrix)
# Eq. 16: φ_{i,i'} = U_{i,i'} / max(U_{i,i}, U_{i',i'})    (Implicit proximity)
# Eq. 17: centrality^implicit_i' = Σ_i φ_{i,i'} / ΣΣ φ_{i,i'}  (Implicit centrality)
# Eq. 18: centrality^explicit_i' = Σ_i ψ_{i,i'} / ΣΣ ψ_{i,i'}  (Explicit centrality)
# Eq. 19: density^implicit_{c,i'} = (Σ_i M_{c,i} * φ_{i,i'}) / (Σ_i φ_{i,i'})  (Implicit density)
# Eq. 20: density^explicit_{c,i'} = (Σ_i M_{c,i} * ψ_{i,i'}) / (Σ_i ψ_{i,i'})  (Explicit density)
# Eq. 21: SI_c = Σ_i d_{c,i}(1 - M_{c,i})ICI_i             (Strategic Index)
# Eq. 22: SG_{c,i} = [Σ_{i'} (φ_{i,i'} / Σ_{i''} φ_{i'',i'}) * (1 - M_{c,i'})ICI_{i'}] - d_{c,i}ICI_i
#         (Strategic Gain)
#
# IMPLEMENTATION NOTES:
# - The original Daboin paper uses M_ci = 1[RCA_ci ≥ 1], but for numerical stability
#   and to match common practice, we use STRICTLY > 1 (excluding exactly 1.0)
# - Sign conventions: ICI negatively correlated with ubiquity; ECI positively with diversity
# - ECI is mathematically equivalent to average ICI of industries with RCA > 1 (up to affine transform)
#
# =============================================================================

# -----------------------------------------------------------------------------
# Load required packages
# -----------------------------------------------------------------------------
library(tidyverse)
library(tigris)
library(readr)
library(stringr)
library(sf)
library(censusapi)
library(readxl)
library(curl)
library(igraph)      # For industry space network visualization
library(ggraph)      # For network plotting
library(Matrix)      # For sparse matrix operations (stress testing)
library(parallel)    # For parallel processing
library(proxy)       # For Jaccard similarity in peer analysis

options(tigris_use_cache = FALSE)

# =============================================================================
# CONFIGURATION: Cross-platform file path detection
# =============================================================================

get_os <- function() {
  sysinf <- Sys.info()
  if (!is.null(sysinf)) {
    os <- sysinf["sysname"]
    if (os == "Darwin") return("mac")
    if (os == "Windows") return("windows")
    if (os == "Linux") return("linux")
  }
  if (.Platform$OS.type == "windows") return("windows")
  return("unix")
}

os_type <- get_os()
cat("Detected operating system:", os_type, "\n")

find_onedrive_path <- function() {
  possible_paths <- c()
  
  if (get_os() == "windows") {
    user_profile <- Sys.getenv("USERPROFILE")
    possible_paths <- c(
      file.path(user_profile, "OneDrive-RMI"),
      file.path(user_profile, "OneDrive - RMI"),
      file.path(Sys.getenv("ONEDRIVE"), "...", "OneDrive-RMI")
    )
  } else {
    home_dir <- Sys.getenv("HOME")
    possible_paths <- c(
      file.path(home_dir, "Library", "CloudStorage", "OneDrive-RMI"),
      file.path(home_dir, "Library", "CloudStorage", "OneDrive - RMI"),
      file.path(home_dir, "OneDrive-RMI"),
      file.path(home_dir, "OneDrive - RMI")
    )
  }
  
  for (path in possible_paths) {
    if (dir.exists(path)) return(path)
  }
  
  warning("Could not find OneDrive-RMI folder. Please set 'onedrive_base' manually.")
  return(NULL)
}

onedrive_base <- find_onedrive_path()
if (is.null(onedrive_base)) stop("OneDrive path not found. Please set 'onedrive_base' variable manually.")

cat("Using OneDrive path:", onedrive_base, "\n")

data_folder <- file.path(
  onedrive_base,
  "US Program - Documents",
  "6_Projects",
  "Clean Regional Economic Development",
  "ACRE",
  "Data",
  "Raw Data"
)

lightcast_file <- file.path(data_folder, "LIGHTCAST_2024_EMPLOYMENT_COUNTY_6DNAICS_GEOID.csv")

# =============================================================================
# PARALLEL PROCESSING CONFIGURATION
# =============================================================================

# Detect number of available cores (use n-1 to leave one for system)
n_cores <- max(1, detectCores() - 1)

# =============================================================================
# OUTPUT VERBOSITY CONFIGURATION
# =============================================================================

# Verbosity levels:
#   0 = minimal (errors only)
#   1 = normal (step summaries, key metrics)
#   2 = verbose (includes glimpse, detailed stats)
#   3 = debug (everything including stress test details)
VERBOSITY <- 1

# Helper functions for conditional output
log_msg <- function(..., level = 1) {
  
  if (VERBOSITY >= level) cat(...)
}

log_step <- function(step_num, step_name) {
  
  if (VERBOSITY >= 1) {
    cat("\n[Step ", step_num, "] ", step_name, "\n", sep = "")
  }
}

log_detail <- function(...) log_msg(..., level = 2)
log_debug <- function(...) log_msg(..., level = 3)

# Compact data summary (replaces verbose glimpse)
data_summary <- function(df, name = "Data", show_glimpse = FALSE) {
  if (VERBOSITY >= 1) {
    cat("  ", name, ": ", format(nrow(df), big.mark = ","), " rows x ", ncol(df), " cols\n", sep = "")
  }
  if (VERBOSITY >= 2 && show_glimpse) {
    glimpse(df)
  }
  invisible(df)
}

# Compact matrix summary
matrix_summary <- function(mat, name = "Matrix") {
  if (VERBOSITY >= 1) {
    fill_rate <- round(100 * mean(mat != 0), 1)
    cat("  ", name, ": ", nrow(mat), " x ", ncol(mat), " (", fill_rate, "% non-zero)\n", sep = "")
  }
  invisible(mat)
}

cat("Parallel processing:", n_cores, "cores | Verbosity level:", VERBOSITY, "\n")

# =============================================================================
# STRESS TEST HELPER FUNCTIONS
# =============================================================================

log_msg("\n========== Defining Helper Functions ==========\n", level = 2)

#' Run a stress test and report results
#' @param test_name Name of the test
#' @param condition Logical condition that should be TRUE for pass
#' @param details Additional details to print
#' @param stop_on_fail Whether to stop execution on failure
stress_test <- function(test_name, condition, details = NULL, stop_on_fail = TRUE) {
  if (condition) {
    if (VERBOSITY >= 3) {
      cat("  ✓", test_name, "\n")
      if (!is.null(details)) cat("    ", details, "\n")
    }
    return(invisible(TRUE))
  } else {
    cat("  ✗ FAIL:", test_name, "\n")
    if (!is.null(details)) cat("    ", details, "\n")
    if (stop_on_fail) {
      stop(paste("Stress test failed:", test_name))
    }
    return(invisible(FALSE))
  }
}

#' Check approximate equality within tolerance
approx_equal <- function(a, b, tol = 1e-10) {
  abs(a - b) < tol
}

#' Check correlation direction
check_correlation <- function(x, y, expected_sign, test_name) {
  cor_val <- cor(x, y, use = "complete.obs")
  if (expected_sign == "positive") {
    condition <- cor_val > 0
    sign_str <- "positive"
  } else {
    condition <- cor_val < 0
    sign_str <- "negative"
  }
  details <- paste("cor =", round(cor_val, 6), ", expected:", sign_str)
  stress_test(test_name, condition, details)
}

# =============================================================================
# VECTORIZED PROXIMITY MATRIX CALCULATION (PERFORMANCE OPTIMIZATION)
# =============================================================================

#' Compute proximity matrix using vectorized operations
#' This is much faster than nested loops
compute_proximity_matrix_vectorized <- function(M_matrix) {
  cat("    Computing co-occurrence matrix U = M^T * M (vectorized)...\n")
  U_matrix <- crossprod(M_matrix)  # Equivalent to t(M) %*% M but faster
  
  cat("    Computing proximity matrix φ (vectorized)...\n")
  # Get diagonal as vector
  diag_U <- diag(U_matrix)
  
  # Create max matrix: max_mat[i,j] = max(U[i,i], U[j,j])
  # Using outer to create matrices and pmax
  max_mat <- pmax(outer(diag_U, rep(1, length(diag_U))),
                  outer(rep(1, length(diag_U)), diag_U))
  
  # φ = U / max(U[i,i], U[j,j])
  phi_matrix <- U_matrix / max_mat
  phi_matrix[is.nan(phi_matrix)] <- 0  # Handle 0/0 cases
  
  return(list(U = U_matrix, phi = phi_matrix))
}

# =============================================================================
# VECTORIZED EDGE LIST CREATION (PERFORMANCE OPTIMIZATION)
# =============================================================================

#' Convert proximity matrix to edge list using vectorized operations
#' This eliminates the stalling issue from nested loops
create_edge_list_vectorized <- function(phi_matrix, ind_codes, top_pct = 0.05) {
  cat("    Creating edge list (vectorized method)...\n")
  
  n_inds <- nrow(phi_matrix)
  
  # Get upper triangle indices (avoid duplicates)
  upper_tri_idx <- which(upper.tri(phi_matrix), arr.ind = TRUE)
  
  # Extract weights for upper triangle
  weights <- phi_matrix[upper_tri_idx]
  
  # Create full edge list
  edge_list <- data.frame(
    from = ind_codes[upper_tri_idx[, 1]],
    to = ind_codes[upper_tri_idx[, 2]],
    weight = weights,
    stringsAsFactors = FALSE
  )
  
  # Filter out zero weights
  edge_list <- edge_list[edge_list$weight > 0, ]
  
  cat("    Total edges (non-zero):", nrow(edge_list), "\n")
  
  # Keep top % of edges by weight
  n_top_edges <- ceiling(nrow(edge_list) * top_pct)
  threshold <- sort(edge_list$weight, decreasing = TRUE)[min(n_top_edges, nrow(edge_list))]
  
  top_edges <- edge_list[edge_list$weight >= threshold, ]
  cat("    Edges after top", top_pct*100, "% filter:", nrow(top_edges), "\n")
  
  # Also keep strongest edge for each node (vectorized)
  # For 'from' column
  strongest_from <- edge_list %>%
    group_by(from) %>%
    slice_max(weight, n = 1, with_ties = FALSE) %>%
    ungroup()
  
  # For 'to' column
  strongest_to <- edge_list %>%
    group_by(to) %>%
    slice_max(weight, n = 1, with_ties = FALSE) %>%
    ungroup()
  
  # Combine and deduplicate
  final_edges <- bind_rows(top_edges, strongest_from, strongest_to) %>%
    distinct(from, to, .keep_all = TRUE)
  
  cat("    Final edges (with strongest per node):", nrow(final_edges), "\n")
  
  return(final_edges)
}

# =============================================================================
# VECTORIZED STRATEGIC GAIN CALCULATION (PERFORMANCE OPTIMIZATION)
# =============================================================================

#' Calculate strategic gain using matrix operations instead of loops
calculate_strategic_gain_vectorized <- function(M_matrix, phi_matrix, 
                                                density_matrix, ici,
                                                phi_col_sums) {
  cat("    Computing Strategic Gain (vectorized)...\n")
  
  n_geos <- nrow(M_matrix)
  n_inds <- ncol(M_matrix)
  
  # Normalized proximity: φ_norm[i, i'] = φ[i, i'] / Σ_{i''} φ[i'', i']
  phi_norm <- t(t(phi_matrix) / phi_col_sums)
  
  # Absence matrix
  absence_matrix <- 1 - M_matrix
  
  # ICI matrix (same ICI repeated for each geography)
  ici_mat <- matrix(ici, nrow = n_geos, ncol = n_inds, byrow = TRUE)
  
  # First term: For each city c and industry i:
  # first_term[c, i] = Σ_{i'} φ_norm[i, i'] * (1 - M[c, i']) * ICI[i']
  # This can be written as: first_term = (φ_norm * ICI) %*% t(absence_matrix) 
  # But we need it transposed, so:
  # Actually, for each i: φ_norm[i, ] * absence[c, ] * ici gives a scalar
  # So first_term = absence_matrix %*% (phi_norm * ici) doesn't work directly
  
  # Let's compute it correctly:
  # first_term[c,i] = sum over i' of: phi_norm[i, i'] * absence[c, i'] * ici[i']
  # = phi_norm[i, ] %*% (absence[c, ] * ici)
  # = phi_norm %*% t(absence * ici_row_for_c)
  
  # For all c at once: this is (absence * ici_mat) %*% t(phi_norm)
  weighted_absence <- absence_matrix * ici_mat  # n_geos x n_inds
  first_term <- weighted_absence %*% t(phi_norm)  # n_geos x n_inds
  
  # Second term: density[c, i] * ICI[i]
  second_term <- density_matrix * ici_mat
  
  # Strategic Gain
  strategic_gain <- first_term - second_term
  
  return(strategic_gain)
}

# =============================================================================
# CHUNKED PARALLEL PROCESSING (MEMORY-SAFE OPERATIONS)
# =============================================================================

log_detail("\n[Defining chunked processing functions]\n")

#' Estimate memory required for a matrix operation
#' @param n_rows Number of rows
#' @param n_cols Number of columns
#' @param bytes_per_element Bytes per matrix element (8 for double)
#' @return Memory estimate in GB
estimate_memory_gb <- function(n_rows, n_cols, bytes_per_element = 8) {
  (n_rows * n_cols * bytes_per_element) / (1024^3)
}

#' Get available system memory in GB
get_available_memory_gb <- function() {
  tryCatch({
    if (.Platform$OS.type == "unix") {
      # Try to get memory info on Unix/Mac
      mem_info <- system("free -b 2>/dev/null || vm_stat 2>/dev/null", intern = TRUE)
      if (length(mem_info) > 0 && grepl("free", mem_info[1], ignore.case = TRUE)) {
        # Linux: parse free output
        mem_line <- strsplit(mem_info[2], "\\s+")[[1]]
        avail_bytes <- as.numeric(mem_line[7])  # "available" column
        return(avail_bytes / (1024^3))
      }
    }
    # Default: assume 8GB available (conservative)
    return(8)
  }, error = function(e) {
    return(8)  # Default fallback
  })
}

#' Calculate optimal chunk size based on available memory
#' @param total_size Total dimension to chunk
#' @param memory_per_element Memory needed per element in GB
#' @param max_memory_fraction Fraction of available memory to use (default 0.5)
#' @return Optimal chunk size
calculate_chunk_size <- function(total_size, memory_per_element_gb, 
                                 max_memory_fraction = 0.5) {
  available_gb <- get_available_memory_gb() * max_memory_fraction
  chunk_size <- floor(sqrt(available_gb / memory_per_element_gb))
  # Ensure reasonable bounds
  chunk_size <- max(100, min(chunk_size, total_size))
  return(chunk_size)
}

#' Compute proximity matrix in chunks to avoid memory limits
#' @param M_matrix Binary M_ci matrix (geographies x industries)
#' @param chunk_size Number of industries to process at once (NULL for auto)
#' @param verbose Print progress messages
#' @return List with U (co-occurrence) and phi (proximity) matrices
compute_proximity_matrix_chunked <- function(M_matrix, chunk_size = NULL, verbose = TRUE) {
  n_inds <- ncol(M_matrix)
  n_geos <- nrow(M_matrix)
  
  # Auto-calculate chunk size if not provided
  if (is.null(chunk_size)) {
    # Memory for chunk: chunk_size^2 * 8 bytes * 3 matrices (U chunk, max chunk, phi chunk)
    memory_per_element <- (8 * 3) / (1024^3)
    chunk_size <- calculate_chunk_size(n_inds, memory_per_element, max_memory_fraction = 0.3)
  }
  
  if (verbose) {
    cat("    Computing proximity matrix in chunks...\n")
    cat("    Matrix dimensions:", n_geos, "geographies x", n_inds, "industries\n")
    cat("    Chunk size:", chunk_size, "industries\n")
    mem_est <- estimate_memory_gb(n_inds, n_inds, 8)
    cat("    Estimated full matrix memory:", round(mem_est, 2), "GB\n")
  }
  
  # If matrix is small enough, use vectorized method
  if (n_inds <= chunk_size) {
    if (verbose) cat("    Using vectorized method (matrix fits in memory)...\n")
    return(compute_proximity_matrix_vectorized(M_matrix))
  }
  
  # Initialize output matrices
  U_matrix <- matrix(0, nrow = n_inds, ncol = n_inds)
  
  # Compute U = M^T * M in chunks
  # U[i,j] = sum over c of M[c,i] * M[c,j]
  n_chunks <- ceiling(n_inds / chunk_size)
  
  if (verbose) cat("    Computing co-occurrence matrix U in", n_chunks, "x", n_chunks, "chunks...\n")
  
  for (i_chunk in 1:n_chunks) {
    i_start <- (i_chunk - 1) * chunk_size + 1
    i_end <- min(i_chunk * chunk_size, n_inds)
    
    for (j_chunk in i_chunk:n_chunks) {  # Only upper triangle + diagonal
      j_start <- (j_chunk - 1) * chunk_size + 1
      j_end <- min(j_chunk * chunk_size, n_inds)
      
      # Compute this chunk of U
      M_i <- M_matrix[, i_start:i_end, drop = FALSE]
      M_j <- M_matrix[, j_start:j_end, drop = FALSE]
      U_chunk <- crossprod(M_i, M_j)  # t(M_i) %*% M_j
      
      # Store in U matrix
      U_matrix[i_start:i_end, j_start:j_end] <- U_chunk
      
      # Mirror for lower triangle (if not on diagonal)
      if (i_chunk != j_chunk) {
        U_matrix[j_start:j_end, i_start:i_end] <- t(U_chunk)
      }
    }
    
    if (verbose && i_chunk %% max(1, n_chunks %/% 5) == 0) {
      cat("      U matrix progress:", round(i_chunk/n_chunks * 100), "%\n")
    }
  }
  
  if (verbose) cat("    Computing proximity matrix phi from U...\n")
  
  # Get diagonal (ubiquity values)
  diag_U <- diag(U_matrix)
  
  # Compute proximity in chunks to avoid creating huge max matrix
  phi_matrix <- matrix(0, nrow = n_inds, ncol = n_inds)
  
  for (i_chunk in 1:n_chunks) {
    i_start <- (i_chunk - 1) * chunk_size + 1
    i_end <- min(i_chunk * chunk_size, n_inds)
    i_range <- i_start:i_end
    
    for (j_chunk in i_chunk:n_chunks) {
      j_start <- (j_chunk - 1) * chunk_size + 1
      j_end <- min(j_chunk * chunk_size, n_inds)
      j_range <- j_start:j_end
      
      # Create max matrix for this chunk
      max_chunk <- pmax(
        outer(diag_U[i_range], rep(1, length(j_range))),
        outer(rep(1, length(i_range)), diag_U[j_range])
      )
      
      # Compute phi chunk
      phi_chunk <- U_matrix[i_range, j_range] / max_chunk
      phi_chunk[is.nan(phi_chunk)] <- 0
      
      phi_matrix[i_range, j_range] <- phi_chunk
      
      if (i_chunk != j_chunk) {
        phi_matrix[j_range, i_range] <- t(phi_chunk)
      }
    }
  }
  
  if (verbose) cat("    Chunked proximity calculation complete.\n")
  
  return(list(U = U_matrix, phi = phi_matrix))
}

#' Compute M-tilde matrix (either M_tilde_C or M_tilde_I) in chunks
#' @param M_matrix Binary M_ci matrix
#' @param row_scale Vector to scale rows (1/diversity for M_tilde_C, 1/ubiquity for M_tilde_I)
#' @param col_scale Vector to scale columns (1/ubiquity for M_tilde_C, 1/diversity for M_tilde_I)
#' @param chunk_size Chunk size (NULL for auto)
#' @param verbose Print progress
#' @return M-tilde matrix
compute_M_tilde_chunked <- function(M_matrix, row_scale, col_scale, 
                                    chunk_size = NULL, verbose = TRUE) {
  n_rows <- nrow(M_matrix)
  n_cols <- ncol(M_matrix)
  
  # Output dimension: M_tilde_C is n_geos x n_geos, M_tilde_I is n_inds x n_inds
  # We're computing M_scaled * M^T where M_scaled has same dims as M
  
  # Determine output size
  out_size <- n_rows  # This is correct for M_tilde_C
  
  if (is.null(chunk_size)) {
    memory_per_element <- (8 * 2) / (1024^3)  # Scaled matrix + output chunk
    chunk_size <- calculate_chunk_size(out_size, memory_per_element, max_memory_fraction = 0.3)
  }
  
  if (verbose) {
    cat("    Computing M-tilde matrix in chunks...\n")
    cat("    Output dimensions:", out_size, "x", out_size, "\n")
    cat("    Chunk size:", chunk_size, "\n")
  }
  
  # Scale M matrix: M_scaled = diag(1/row_scale) * M * diag(1/col_scale)
  # Row scaling: divide each row by row_scale
  # Col scaling: divide each column by col_scale
  
  # If small enough, compute directly
  if (out_size <= chunk_size) {
    if (verbose) cat("    Using direct computation (fits in memory)...\n")
    M_col_scaled <- t(t(M_matrix) / col_scale)
    M_row_scaled <- M_col_scaled / row_scale
    M_tilde <- M_row_scaled %*% t(M_matrix)
    return(M_tilde)
  }
  
  # Chunked computation
  M_tilde <- matrix(0, nrow = out_size, ncol = out_size)
  n_chunks <- ceiling(out_size / chunk_size)
  
  if (verbose) cat("    Processing", n_chunks, "row chunks...\n")
  
  for (i_chunk in 1:n_chunks) {
    i_start <- (i_chunk - 1) * chunk_size + 1
    i_end <- min(i_chunk * chunk_size, out_size)
    i_range <- i_start:i_end
    
    # Get rows of M for this chunk
    M_chunk <- M_matrix[i_range, , drop = FALSE]
    row_scale_chunk <- row_scale[i_range]
    
    # Scale this chunk
    M_col_scaled_chunk <- t(t(M_chunk) / col_scale)
    M_row_scaled_chunk <- M_col_scaled_chunk / row_scale_chunk
    
    # Multiply by full M^T (result is chunk_rows x n_rows)
    M_tilde_chunk <- M_row_scaled_chunk %*% t(M_matrix)
    
    M_tilde[i_range, ] <- M_tilde_chunk
    
    if (verbose && i_chunk %% max(1, n_chunks %/% 5) == 0) {
      cat("      Progress:", round(i_chunk/n_chunks * 100), "%\n")
    }
  }
  
  if (verbose) cat("    Chunked M-tilde computation complete.\n")
  
  return(M_tilde)
}

#' Create edge list from proximity matrix in chunks (memory-safe)
#' @param phi_matrix Proximity matrix
#' @param ind_codes Industry codes
#' @param top_pct Top percentage of edges to keep
#' @param chunk_size Chunk size for processing
#' @return Edge list data frame
create_edge_list_chunked <- function(phi_matrix, ind_codes, top_pct = 0.05, 
                                     chunk_size = 5000, verbose = TRUE) {
  n_inds <- nrow(phi_matrix)
  
  if (verbose) {
    cat("    Creating edge list in chunks...\n")
    cat("    Matrix size:", n_inds, "x", n_inds, "\n")
    total_edges <- n_inds * (n_inds - 1) / 2
    cat("    Maximum possible edges (upper triangle):", format(total_edges, big.mark = ","), "\n")
  }
  
  # If small, use vectorized method
  if (n_inds <= chunk_size) {
    if (verbose) cat("    Using vectorized method...\n")
    return(create_edge_list_vectorized(phi_matrix, ind_codes, top_pct))
  }
  
  # Process in chunks, collecting non-zero edges
  edge_list_chunks <- list()
  n_chunks <- ceiling(n_inds / chunk_size)
  chunk_idx <- 1
  
  if (verbose) cat("    Processing", n_chunks * (n_chunks + 1) / 2, "chunk pairs...\n")
  
  for (i_chunk in 1:n_chunks) {
    i_start <- (i_chunk - 1) * chunk_size + 1
    i_end <- min(i_chunk * chunk_size, n_inds)
    i_range <- i_start:i_end
    
    for (j_chunk in i_chunk:n_chunks) {
      j_start <- (j_chunk - 1) * chunk_size + 1
      j_end <- min(j_chunk * chunk_size, n_inds)
      j_range <- j_start:j_end
      
      # Get this chunk of the matrix
      phi_chunk <- phi_matrix[i_range, j_range, drop = FALSE]
      
      # Get upper triangle indices (or all if different chunks)
      if (i_chunk == j_chunk) {
        # Same chunk - only upper triangle
        tri_idx <- which(upper.tri(phi_chunk), arr.ind = TRUE)
        weights <- phi_chunk[tri_idx]
        from_idx <- i_range[tri_idx[, 1]]
        to_idx <- j_range[tri_idx[, 2]]
      } else {
        # Different chunks - all pairs (but i < j by chunk order)
        all_idx <- expand.grid(row = seq_along(i_range), col = seq_along(j_range))
        weights <- phi_chunk[as.matrix(all_idx)]
        from_idx <- i_range[all_idx$row]
        to_idx <- j_range[all_idx$col]
      }
      
      # Keep only non-zero weights
      nonzero <- weights > 0
      if (any(nonzero)) {
        edge_list_chunks[[chunk_idx]] <- data.frame(
          from = ind_codes[from_idx[nonzero]],
          to = ind_codes[to_idx[nonzero]],
          weight = weights[nonzero],
          stringsAsFactors = FALSE
        )
        chunk_idx <- chunk_idx + 1
      }
    }
  }
  
  if (verbose) cat("    Combining", length(edge_list_chunks), "edge chunks...\n")
  
  # Combine all chunks
  edge_list <- bind_rows(edge_list_chunks)
  
  if (verbose) cat("    Total non-zero edges:", format(nrow(edge_list), big.mark = ","), "\n")
  
  # Apply filtering (same as vectorized version)
  n_top_edges <- ceiling(nrow(edge_list) * top_pct)
  threshold <- sort(edge_list$weight, decreasing = TRUE)[min(n_top_edges, nrow(edge_list))]
  
  top_edges <- edge_list[edge_list$weight >= threshold, ]
  if (verbose) cat("    Edges after top", top_pct*100, "% filter:", nrow(top_edges), "\n")
  
  # Keep strongest edge for each node
  strongest_from <- edge_list %>%
    group_by(from) %>%
    slice_max(weight, n = 1, with_ties = FALSE) %>%
    ungroup()
  
  strongest_to <- edge_list %>%
    group_by(to) %>%
    slice_max(weight, n = 1, with_ties = FALSE) %>%
    ungroup()
  
  final_edges <- bind_rows(top_edges, strongest_from, strongest_to) %>%
    distinct(from, to, .keep_all = TRUE)
  
  if (verbose) cat("    Final edges:", format(nrow(final_edges), big.mark = ","), "\n")
  
  return(final_edges)
}

#' Compute strategic gain in chunks (memory-safe)
#' @param M_matrix Binary M_ci matrix
#' @param phi_matrix Proximity matrix
#' @param density_matrix Density matrix
#' @param ici Industry complexity values
#' @param phi_col_sums Column sums of proximity matrix
#' @param chunk_size Chunk size for geography processing
#' @return Strategic gain matrix
calculate_strategic_gain_chunked <- function(M_matrix, phi_matrix, 
                                             density_matrix, ici, phi_col_sums,
                                             chunk_size = 500, verbose = TRUE) {
  n_geos <- nrow(M_matrix)
  n_inds <- ncol(M_matrix)
  
  if (verbose) {
    cat("    Computing Strategic Gain in chunks...\n")
    cat("    Dimensions:", n_geos, "geographies x", n_inds, "industries\n")
    cat("    Chunk size:", chunk_size, "geographies\n")
  }
  
  # If small, use vectorized
  if (n_geos <= chunk_size) {
    if (verbose) cat("    Using vectorized method...\n")
    return(calculate_strategic_gain_vectorized(M_matrix, phi_matrix, 
                                               density_matrix, ici, phi_col_sums))
  }
  
  # Precompute normalized proximity (this is n_inds x n_inds, should fit)
  phi_norm <- t(t(phi_matrix) / phi_col_sums)
  
  # Process in chunks by geography
  strategic_gain <- matrix(0, nrow = n_geos, ncol = n_inds)
  n_chunks <- ceiling(n_geos / chunk_size)
  
  for (i_chunk in 1:n_chunks) {
    i_start <- (i_chunk - 1) * chunk_size + 1
    i_end <- min(i_chunk * chunk_size, n_geos)
    i_range <- i_start:i_end
    chunk_n <- length(i_range)
    
    # Get chunk data
    M_chunk <- M_matrix[i_range, , drop = FALSE]
    density_chunk <- density_matrix[i_range, , drop = FALSE]
    
    # Absence matrix for this chunk
    absence_chunk <- 1 - M_chunk
    
    # ICI matrix for this chunk
    ici_mat_chunk <- matrix(ici, nrow = chunk_n, ncol = n_inds, byrow = TRUE)
    
    # First term: weighted_absence %*% t(phi_norm)
    weighted_absence_chunk <- absence_chunk * ici_mat_chunk
    first_term_chunk <- weighted_absence_chunk %*% t(phi_norm)
    
    # Second term: density * ICI
    second_term_chunk <- density_chunk * ici_mat_chunk
    
    # Strategic gain for this chunk
    strategic_gain[i_range, ] <- first_term_chunk - second_term_chunk
    
    if (verbose && i_chunk %% max(1, n_chunks %/% 5) == 0) {
      cat("      Progress:", round(i_chunk/n_chunks * 100), "%\n")
    }
  }
  
  if (verbose) cat("    Chunked strategic gain computation complete.\n")
  
  return(strategic_gain)
}

#' Wrapper to choose between vectorized and chunked methods based on matrix size
#' @param method_name Name of the computation
#' @param n_elements Number of elements in the primary dimension
#' @param size_threshold Size above which to use chunked method
#' @param use_chunked Force chunked method (NULL for auto-detect)
#' @param verbose Print progress
#' @return Boolean indicating whether to use chunked method
auto_chunk_method <- function(method_name, n_elements, size_threshold = 5000, 
                              use_chunked = NULL, verbose = TRUE) {
  # Auto-detect if not specified
  if (is.null(use_chunked)) {
    mem_estimate <- estimate_memory_gb(n_elements, n_elements, 8)
    available_mem <- get_available_memory_gb()
    use_chunked <- (n_elements > size_threshold) || (mem_estimate > available_mem * 0.3)
  }
  
  if (verbose && use_chunked) {
    cat("    [Auto-selected chunked method for", method_name, "]\n")
    cat("    Reason: n =", n_elements, ", estimated memory =", 
        round(estimate_memory_gb(n_elements, n_elements, 8), 2), "GB\n")
  }
  
  return(use_chunked)
}

# =============================================================================
# COMPLEXITY CALCULATION FUNCTIONS (DABOIN METHOD - FULLY DOCUMENTED)
# =============================================================================

log_detail("\n[Defining complexity functions]\n")

#' Calculate Economic Complexity metrics using Daboin methodology with full validation
#'
#' This function implements Equations 1-14 from the Daboin paper with comprehensive
#' stress testing at each step to ensure mathematical correctness.
#'
#' @param empshare_lq_data Data frame with geoid, industry_code, and industry_lq columns
#' @param geo_level_name Name of the geographic level for reporting
#' @param run_stress_tests Whether to run stress tests (default TRUE)
#' @param create_industry_space Whether to compute proximity and create industry space (default TRUE)
#' @return List containing diversity, ubiquity, eci, ici, proximity, and diagnostics
#'
calculate_complexity_metrics <- function(empshare_lq_data, 
                                         geo_level_name = "geography",
                                         run_stress_tests = TRUE,
                                         create_industry_space = TRUE) {
  
  log_msg("\n[Complexity] ", toupper(geo_level_name), "\n")
  
  # =========================================================================
  # STEP 1: INPUT VALIDATION AND DATA PREPARATION
  # =========================================================================
  
  empshare_lq_data <- empshare_lq_data %>%
    mutate(industry_lq = replace_na(industry_lq, 0))
  
  n_geos_input <- n_distinct(empshare_lq_data$geoid)
  n_inds_input <- n_distinct(empshare_lq_data$industry_code)
  n_rows_input <- nrow(empshare_lq_data)
  
  log_msg("  Input: ", n_geos_input, " geos x ", n_inds_input, " industries\n")
  
  if (run_stress_tests) {
    stress_test("No NA in LQ", sum(is.na(empshare_lq_data$industry_lq)) == 0)
    stress_test("LQ >= 0", all(empshare_lq_data$industry_lq >= 0))
    stress_test("Complete matrix", n_rows_input == n_geos_input * n_inds_input,
                paste(n_rows_input, "vs", n_geos_input * n_inds_input))
  }
  
  # =========================================================================
  # STEP 2: CONSTRUCT BINARY SPECIALIZATION MATRIX M_ci (Equation 5)
  # =========================================================================
  
  M_data <- empshare_lq_data %>%
    mutate(M_ci = as.integer(industry_lq > 1)) %>%
    select(geoid, industry_code, M_ci)
  
  n_specializations <- sum(M_data$M_ci)
  fill_rate <- 100 * n_specializations / nrow(M_data)
  
  log_msg("  M_ci: ", round(fill_rate, 1), "% fill rate (", n_specializations, " specializations)\n")
  
  if (run_stress_tests) {
    stress_test("M_ci binary", all(M_data$M_ci %in% c(0, 1)))
    stress_test("M_ci sum check", sum(M_data$M_ci) == sum(empshare_lq_data$industry_lq > 1))
    stress_test("Fill rate reasonable", fill_rate > 5 && fill_rate < 50,
                paste(round(fill_rate, 1), "%"))
  }
  
  # =========================================================================
  # STEP 3: CALCULATE DIVERSITY K_c0 (Equation 6)
  # =========================================================================
  
  diversity <- M_data %>%
    group_by(geoid) %>%
    summarise(diversity = sum(M_ci), .groups = "drop")
  
  log_detail("  Diversity: min=", min(diversity$diversity), " med=", median(diversity$diversity),
             " max=", max(diversity$diversity), "\n")
  
  if (run_stress_tests) {
    stress_test("Diversity >= 0", all(diversity$diversity >= 0))
    stress_test("Sum diversity check", sum(diversity$diversity) == n_specializations)
  }
  
  # =========================================================================
  # STEP 4: CALCULATE UBIQUITY K_i0 (Equation 7)
  # =========================================================================
  
  ubiquity <- M_data %>%
    group_by(industry_code) %>%
    summarise(ubiquity = sum(M_ci), .groups = "drop")
  
  log_detail("  Ubiquity: min=", min(ubiquity$ubiquity), " med=", median(ubiquity$ubiquity),
             " max=", max(ubiquity$ubiquity), "\n")
  
  if (run_stress_tests) {
    stress_test("Ubiquity >= 0", all(ubiquity$ubiquity >= 0))
    stress_test("Sum(div)==Sum(ubiq)", sum(diversity$diversity) == sum(ubiquity$ubiquity))
  }
  
  # =========================================================================
  # STEP 5: FILTER FOR VALID GEOGRAPHIES AND INDUSTRIES
  # =========================================================================
  
  valid_geos <- diversity %>% filter(diversity > 0) %>% pull(geoid)
  valid_inds <- ubiquity %>% filter(ubiquity > 0) %>% pull(industry_code)
  
  removed_geos <- n_geos_input - length(valid_geos)
  removed_inds <- n_inds_input - length(valid_inds)
  
  if (removed_geos > 0 || removed_inds > 0) {
    log_msg("  Filtered: -", removed_geos, " geos, -", removed_inds, " industries\n")
  }
  
  M_filtered <- M_data %>% 
    filter(geoid %in% valid_geos, industry_code %in% valid_inds)
  
  if (run_stress_tests) {
    stress_test(">=90% geos retained", length(valid_geos) >= 0.9 * n_geos_input)
    stress_test(">=90% industries retained", length(valid_inds) >= 0.9 * n_inds_input)
  }
  
  # =========================================================================
  # STEP 6: BUILD M MATRIX (rows = geographies, columns = industries)
  # =========================================================================
  
  M_wide <- M_filtered %>%
    pivot_wider(names_from = industry_code, values_from = M_ci, values_fill = 0)
  
  geo_ids <- M_wide$geoid
  ind_codes <- colnames(M_wide)[-1]
  
  M_matrix <- as.matrix(M_wide %>% select(-geoid))
  rownames(M_matrix) <- geo_ids
  colnames(M_matrix) <- ind_codes
  
  n_geos <- nrow(M_matrix)
  n_inds <- ncol(M_matrix)
  
  log_msg("  M matrix: ", n_geos, " x ", n_inds, " (density: ", 
          round(sum(M_matrix) / (n_geos * n_inds), 3), ")\n")
  
  diversity_vec <- diversity %>%
    filter(geoid %in% geo_ids) %>%
    arrange(match(geoid, geo_ids)) %>%
    pull(diversity)
  
  ubiquity_vec <- ubiquity %>%
    filter(industry_code %in% ind_codes) %>%
    arrange(match(industry_code, ind_codes)) %>%
    pull(ubiquity)
  
  if (run_stress_tests) {
    stress_test("Diversity vec length", length(diversity_vec) == n_geos)
    stress_test("Ubiquity vec length", length(ubiquity_vec) == n_inds)
    stress_test("Row sums = diversity", all(rowSums(M_matrix) == diversity_vec))
    stress_test("Col sums = ubiquity", all(colSums(M_matrix) == ubiquity_vec))
  }
  
  # =========================================================================
  # STEP 7: CALCULATE AVERAGE UBIQUITY K_c1 (Equation 8)
  # =========================================================================
  
  K_c1 <- (M_matrix %*% ubiquity_vec) / diversity_vec
  
  if (run_stress_tests) {
    first_geo_idx <- 1
    first_geo_specializations <- which(M_matrix[first_geo_idx, ] == 1)
    manual_K_c1 <- mean(ubiquity_vec[first_geo_specializations])
    stress_test("K_c1 manual verification",
                approx_equal(K_c1[first_geo_idx], manual_K_c1),
                paste("Computed:", round(K_c1[first_geo_idx], 6), 
                      "Manual:", round(manual_K_c1, 6)))
    stress_test("K_c1 bounded", all(K_c1 >= min(ubiquity_vec) & K_c1 <= max(ubiquity_vec)))
  }
  
  # =========================================================================
  # STEP 8: CALCULATE AVERAGE DIVERSITY K_i1 (Equation 9)
  # =========================================================================
  
  K_i1 <- (t(M_matrix) %*% diversity_vec) / ubiquity_vec
  
  if (run_stress_tests) {
    first_ind_idx <- 1
    first_ind_cities <- which(M_matrix[, first_ind_idx] == 1)
    manual_K_i1 <- mean(diversity_vec[first_ind_cities])
    stress_test("K_i1 manual verification", approx_equal(K_i1[first_ind_idx], manual_K_i1))
    stress_test("K_i1 bounded", all(K_i1 >= min(diversity_vec) & K_i1 <= max(diversity_vec)))
  }
  
  # =========================================================================
  # STEP 9: BUILD M̃^C MATRIX AND COMPUTE ECI (Equations 12-14)
  # =========================================================================
  
  use_chunked_MC <- auto_chunk_method("M_tilde_C", n_geos, size_threshold = 3000)
  
  if (use_chunked_MC) {
    log_detail("  Using chunked computation for M̃^C...\n")
    M_tilde_C <- compute_M_tilde_chunked(
      M_matrix, row_scale = diversity_vec, col_scale = ubiquity_vec
    )
  } else {
    M_col_scaled <- t(t(M_matrix) / ubiquity_vec)
    M_row_scaled <- M_col_scaled / diversity_vec
    M_tilde_C <- M_row_scaled %*% t(M_matrix)
  }
  
  if (run_stress_tests) {
    row_sums_MC <- rowSums(M_tilde_C)
    stress_test("M̃^C rows sum to 1", all(approx_equal(row_sums_MC, 1)))
    stress_test("M̃^C non-negative", all(M_tilde_C >= 0))
    if (n_geos >= 2) {
      manual_MC_12 <- sum((M_matrix[1,] * M_matrix[2,]) / (diversity_vec[1] * ubiquity_vec))
      stress_test("M̃^C[1,2] check", approx_equal(M_tilde_C[1, 2], manual_MC_12))
    }
  }
  
  # Eigenvalue decomposition
  eigen_C <- eigen(M_tilde_C, symmetric = FALSE)
  vals_C <- Re(eigen_C$values)
  ord_C <- order(vals_C, decreasing = TRUE)
  
  first_eval_C <- vals_C[ord_C[1]]
  second_eval_C <- vals_C[ord_C[2]]
  third_eval_C <- vals_C[ord_C[3]]
  
  log_detail("  ECI eigenvalues: λ1=", round(first_eval_C, 6), " λ2=", round(second_eval_C, 6), "\n")
  
  eci_raw <- Re(eigen_C$vectors[, ord_C[2]])
  
  if (run_stress_tests) {
    stress_test("λ1 ≈ 1", approx_equal(first_eval_C, 1, tol = 1e-6))
    stress_test("λ2 < 1", second_eval_C < 1)
    Mv2 <- M_tilde_C %*% eci_raw
    lambda2_v2 <- second_eval_C * eci_raw
    stress_test("Eigenvector equation", max(abs(Mv2 - lambda2_v2)) < 1e-10)
  }
  
  # =========================================================================
  # STEP 10: BUILD M̃^I MATRIX AND COMPUTE ICI
  # =========================================================================
  
  use_chunked_MI <- auto_chunk_method("M_tilde_I", n_inds, size_threshold = 2000)
  
  if (use_chunked_MI) {
    log_detail("  Using chunked computation for M̃^I...\n")
    M_T <- t(M_matrix)
    M_tilde_I <- compute_M_tilde_chunked(M_T, row_scale = ubiquity_vec, col_scale = diversity_vec)
  } else {
    M_row_scaled_for_I <- M_matrix / diversity_vec
    M_T_Dinv <- t(M_row_scaled_for_I)
    M_T_Dinv_Uinv <- M_T_Dinv / ubiquity_vec
    M_tilde_I <- M_T_Dinv_Uinv %*% M_matrix
  }
  
  if (run_stress_tests) {
    row_sums_MI <- rowSums(M_tilde_I)
    stress_test("M̃^I rows sum to 1", all(approx_equal(row_sums_MI, 1)))
    stress_test("M̃^I non-negative", all(M_tilde_I >= 0))
  }
  
  eigen_I <- eigen(M_tilde_I, symmetric = FALSE)
  vals_I <- Re(eigen_I$values)
  ord_I <- order(vals_I, decreasing = TRUE)
  
  first_eval_I <- vals_I[ord_I[1]]
  second_eval_I <- vals_I[ord_I[2]]
  third_eval_I <- vals_I[ord_I[3]]
  
  log_detail("  ICI eigenvalues: λ1=", round(first_eval_I, 6), " λ2=", round(second_eval_I, 6), "\n")
  
  ici_raw <- Re(eigen_I$vectors[, ord_I[2]])
  
  if (run_stress_tests) {
    stress_test("M̃^I λ1 ≈ 1", approx_equal(first_eval_I, 1, tol = 1e-6))
    stress_test("Shared λ2", approx_equal(second_eval_C, second_eval_I, tol = 1e-6))
  }
  
  # =========================================================================
  # STEP 11: SIGN ANCHORING AND STANDARDIZATION
  # =========================================================================
  
  # Check and fix ICI sign (should be negatively correlated with ubiquity)
  ici_ubiq_corr_raw <- cor(ici_raw, ubiquity_vec)
  if (ici_ubiq_corr_raw > 0) {
    ici_raw <- -ici_raw
  }
  
  # Check and fix ECI sign (should be positively correlated with diversity)
  eci_div_corr_raw <- cor(eci_raw, diversity_vec)
  if (eci_div_corr_raw < 0) {
    eci_raw <- -eci_raw
  }
  
  # Standardize to mean=0, sd=1
  eci <- (eci_raw - mean(eci_raw)) / sd(eci_raw)
  ici <- (ici_raw - mean(ici_raw)) / sd(ici_raw)
  
  if (run_stress_tests) {
    final_ici_ubiq_corr <- cor(ici, ubiquity_vec)
    final_eci_div_corr <- cor(eci, diversity_vec)
    stress_test("ICI neg corr with ubiquity", final_ici_ubiq_corr < 0)
    stress_test("ECI pos corr with diversity", final_eci_div_corr > 0)
    stress_test("ECI mean ≈ 0", approx_equal(mean(eci), 0, tol = 1e-10))
    stress_test("ICI mean ≈ 0", approx_equal(mean(ici), 0, tol = 1e-10))
  }
  
  # =========================================================================
  # STEP 12: VERIFY ECI ≈ AVERAGE ICI
  # =========================================================================
  
  avg_ici_by_geo <- vapply(seq_len(n_geos), function(c_idx) {
    specs <- which(M_matrix[c_idx, ] == 1)
    if (length(specs) == 0) return(NA_real_)
    mean(ici[specs])
  }, numeric(1))
  
  eci_avgici_corr <- cor(eci, avg_ici_by_geo, use = "complete.obs")
  
  if (run_stress_tests) {
    stress_test("|cor(ECI, avgICI)| > 0.99", abs(eci_avgici_corr) > 0.99,
                paste("cor =", round(eci_avgici_corr, 4)))
  }
  
  # =========================================================================
  # STEP 13: PROXIMITY MATRIX φ (Equation 15-16) - INDUSTRY SPACE
  # =========================================================================
  proximity_results <- NULL
  
  if (create_industry_space) {
    use_chunked <- auto_chunk_method("proximity matrix", n_inds, size_threshold = 2000)
    if (use_chunked) {
      prox_result <- compute_proximity_matrix_chunked(M_matrix)
    } else {
      prox_result <- compute_proximity_matrix_vectorized(M_matrix)
    }
    U_matrix <- prox_result$U
    phi_matrix <- prox_result$phi
    
    rownames(phi_matrix) <- ind_codes
    colnames(phi_matrix) <- ind_codes
    rownames(U_matrix) <- ind_codes
    colnames(U_matrix) <- ind_codes
    
    log_detail("  φ matrix: ", nrow(phi_matrix), "x", ncol(phi_matrix), 
               " range [", round(min(phi_matrix), 3), ",", round(max(phi_matrix), 3), "]\n")
    
    if (run_stress_tests) {
      stress_test("U symmetric", isSymmetric(U_matrix))
      stress_test("U diag = ubiquity", all(diag(U_matrix) == ubiquity_vec))
      stress_test("φ symmetric", isSymmetric(phi_matrix))
      stress_test("φ diag = 1", all(approx_equal(diag(phi_matrix), 1)))
      stress_test("φ in [0,1]", all(phi_matrix >= 0 & phi_matrix <= 1))
    }
    
    # =========================================================================
    # STEP 14: INDUSTRY CENTRALITY (Equation 17)
    # =========================================================================
    
    phi_row_sums <- rowSums(phi_matrix)
    phi_total_sum <- sum(phi_matrix)
    centrality_implicit <- phi_row_sums / phi_total_sum
    
    if (run_stress_tests) {
      stress_test("Centrality sums to 1", approx_equal(sum(centrality_implicit), 1))
    }
    
    # =========================================================================
    # STEP 15: COLOCATION DENSITY (Equation 19)
    # =========================================================================
    
    phi_col_sums <- colSums(phi_matrix)
    density_matrix <- (M_matrix %*% phi_matrix) / matrix(phi_col_sums, nrow = n_geos, ncol = n_inds, byrow = TRUE)
    rownames(density_matrix) <- geo_ids
    colnames(density_matrix) <- ind_codes
    
    log_detail("  Density matrix: [", round(min(density_matrix), 3), ",", 
               round(max(density_matrix), 3), "] mean=", round(mean(density_matrix), 3), "\n")
    
    if (run_stress_tests) {
      stress_test("Density in [0,1]", all(density_matrix >= 0 & density_matrix <= 1))
      present_density <- mean(density_matrix[M_matrix == 1])
      absent_density <- mean(density_matrix[M_matrix == 0])
      stress_test("Present > absent density", present_density > absent_density)
    }
    
    # =========================================================================
    # STEP 16: STRATEGIC INDEX (Equation 21)
    # =========================================================================
    
    ici_matrix <- matrix(ici, nrow = n_geos, ncol = n_inds, byrow = TRUE)
    absence_matrix <- 1 - M_matrix
    strategic_index <- rowSums(density_matrix * absence_matrix * ici_matrix)
    names(strategic_index) <- geo_ids
    
    # =========================================================================
    # STEP 17: STRATEGIC GAIN (Equation 22)
    # =========================================================================
    
    use_chunked_sg <- auto_chunk_method("strategic gain", n_geos, size_threshold = 3000)
    if (use_chunked_sg) {
      strategic_gain <- calculate_strategic_gain_chunked(
        M_matrix, phi_matrix, density_matrix, ici, phi_col_sums
      )
    } else {
      strategic_gain <- calculate_strategic_gain_vectorized(
        M_matrix, phi_matrix, density_matrix, ici, phi_col_sums
      )
    }
    
    rownames(strategic_gain) <- geo_ids
    colnames(strategic_gain) <- ind_codes
    
    # Store proximity results
    proximity_results <- list(
      co_occurrence = U_matrix,
      proximity = phi_matrix,
      centrality = centrality_implicit,
      density = density_matrix,
      strategic_index = strategic_index,
      strategic_gain = strategic_gain
    )
  }
  
  # =========================================================================
  # FINAL DIAGNOSTICS
  # =========================================================================
  
  final_ici_ubiq_corr <- cor(ici, ubiquity_vec)
  final_eci_div_corr <- cor(eci, diversity_vec)
  ici_sign_flipped <- ici_ubiq_corr_raw > 0
  eci_sign_flipped <- eci_div_corr_raw < 0
  
  log_msg("  ECI range: [", round(min(eci), 2), ", ", round(max(eci), 2), 
          "] | ICI range: [", round(min(ici), 2), ", ", round(max(ici), 2), "]\n")
  log_msg("  cor(ICI,ubiq)=", round(final_ici_ubiq_corr, 3), 
          " cor(ECI,div)=", round(final_eci_div_corr, 3), 
          " cor(ECI,avgICI)=", round(eci_avgici_corr, 3), "\n")
  
  # =========================================================================
  # PREPARE OUTPUT
  # =========================================================================
  
  eci_df <- tibble(geoid = geo_ids, economic_complexity_index = eci)
  ici_df <- tibble(industry_code = ind_codes, industry_complexity_index = ici)
  diversity_df <- tibble(geoid = geo_ids, diversity = diversity_vec)
  ubiquity_df <- tibble(industry_code = ind_codes, ubiquity = ubiquity_vec)
  K_c1_df <- tibble(geoid = geo_ids, avg_ubiquity = as.vector(K_c1))
  K_i1_df <- tibble(industry_code = ind_codes, avg_diversity = as.vector(K_i1))
  avg_ici_df <- tibble(geoid = geo_ids, avg_ici = avg_ici_by_geo)
  
  return(list(
    eci = eci_df,
    ici = ici_df,
    diversity = diversity_df,
    ubiquity = ubiquity_df,
    K_c1 = K_c1_df,
    K_i1 = K_i1_df,
    avg_ici = avg_ici_df,
    M_matrix = M_matrix,
    proximity = proximity_results,
    diagnostics = list(
      ici_ubiquity_correlation = final_ici_ubiq_corr,
      eci_diversity_correlation = final_eci_div_corr,
      eci_avgici_correlation = eci_avgici_corr,
      first_eigenvalue_C = first_eval_C,
      second_eigenvalue_C = second_eval_C,
      third_eigenvalue_C = third_eval_C,
      first_eigenvalue_I = first_eval_I,
      second_eigenvalue_I = second_eval_I,
      third_eigenvalue_I = third_eval_I,
      ici_sign_flipped = ici_sign_flipped,
      eci_sign_flipped = eci_sign_flipped,
      n_geos = n_geos,
      n_inds = n_inds,
      fill_rate = fill_rate
    )
  ))
}

# =============================================================================
# INDUSTRY SPACE VISUALIZATION FUNCTION (OPTIMIZED)
# =============================================================================

#' Create industry space network visualization
#'
#' Creates a network graph showing the co-location proximity between industries.
#' Retains top 5% of connections plus the strongest connection for each node.
#' OPTIMIZED: Uses vectorized edge list creation.
#'
#' @param complexity_results Output from calculate_complexity_metrics()
#' @param industry_names Data frame with industry_code and industry_description
#' @param geo_level_name Name for plot title
#' @param top_pct Percentage of strongest edges to keep (default 5%)
#' @return ggplot object
#'
create_industry_space_plot <- function(complexity_results, industry_names, 
                                       geo_level_name, top_pct = 0.05) {
  
  cat("\n====================================================================\n")
  cat("Creating Industry Space Visualization for:", toupper(geo_level_name), "\n")
  cat("====================================================================\n")
  
  if (is.null(complexity_results$proximity)) {
    cat("  ERROR: Proximity not computed. Set create_industry_space = TRUE\n")
    return(NULL)
  }
  
  phi_matrix <- complexity_results$proximity$proximity
  ici <- complexity_results$ici$industry_complexity_index
  ind_codes <- complexity_results$ici$industry_code
  
  n_inds <- nrow(phi_matrix)
  
  # Use chunked edge list creation if matrix is large, otherwise vectorized
  use_chunked_edges <- auto_chunk_method("edge list", n_inds, size_threshold = 3000)
  if (use_chunked_edges) {
    final_edges <- create_edge_list_chunked(phi_matrix, ind_codes, top_pct)
  } else {
    final_edges <- create_edge_list_vectorized(phi_matrix, ind_codes, top_pct)
  }
  
  # Create igraph object
  g <- graph_from_data_frame(final_edges, directed = FALSE)
  
  # Add node attributes
  node_df <- tibble(industry_code = V(g)$name) %>%
    left_join(
      complexity_results$ici %>% 
        rename(industry_code = industry_code, ICI = industry_complexity_index),
      by = "industry_code"
    ) %>%
    left_join(
      complexity_results$ubiquity %>% 
        rename(industry_code = industry_code, ubiq = ubiquity),
      by = "industry_code"
    ) %>%
    left_join(industry_names, by = "industry_code")
  
  V(g)$ICI <- node_df$ICI
  V(g)$ubiquity <- node_df$ubiq
  V(g)$label <- substr(node_df$industry_description, 1, 20)
  
  # Create plot
  cat("  Creating network plot...\n")
  
  p <- ggraph(g, layout = "fr") +
    geom_edge_link(aes(alpha = weight), color = "gray70", show.legend = FALSE) +
    geom_node_point(aes(color = ICI, size = ubiquity), alpha = 0.8) +
    scale_color_viridis_c(option = "plasma", name = "ICI") +
    scale_size_continuous(range = c(1, 6), name = "Ubiquity") +
    labs(
      title = paste("Industry Space:", geo_level_name),
      subtitle = paste("Showing top", top_pct*100, "% of proximity connections + strongest per industry"),
      caption = "Node size = ubiquity, Node color = Industry Complexity Index"
    ) +
    theme_void() +
    theme(
      plot.title = element_text(hjust = 0.5, size = 14, face = "bold"),
      plot.subtitle = element_text(hjust = 0.5, size = 10),
      legend.position = "right"
    )
  
  cat("  Industry space visualization complete.\n")
  
  return(p)
}

# =============================================================================
# PEER GEOGRAPHY ANALYSIS FUNCTIONS
# =============================================================================

#' Calculate Jaccard similarity between geographies based on industry specializations
#'
#' @param M_matrix Binary specialization matrix (rows = geographies, cols = industries)
#' @return Symmetric Jaccard similarity matrix
#'
calculate_geography_similarity <- function(M_matrix) {
  # Jaccard similarity: |A ∩ B| / |A ∪ B|
  # For binary matrices: uses proxy package for efficient computation
  similarity_matrix <- as.matrix(proxy::simil(M_matrix, method = "Jaccard"))
  rownames(similarity_matrix) <- rownames(M_matrix)
  colnames(similarity_matrix) <- rownames(M_matrix)
  return(similarity_matrix)
}

#' Get top N most similar geographies for a given geography
#'
#' @param target_geoid The target geography ID
#' @param similarity_matrix Jaccard similarity matrix
#' @param n Number of similar geographies to return (default 5)
#' @return Data frame with similar geographies and similarity scores
#'
get_similar_geographies <- function(target_geoid, similarity_matrix, n = 5) {
  if (!target_geoid %in% rownames(similarity_matrix)) {
    warning(paste("Geography not found:", target_geoid))
    return(NULL)
  }
  
  similarities <- similarity_matrix[target_geoid, ]
  similarities <- similarities[names(similarities) != target_geoid]  # Remove self
  top_similar <- sort(similarities, decreasing = TRUE)[1:min(n, length(similarities))]
  
  return(data.frame(
    peer_geoid = names(top_similar),
    jaccard_similarity = as.vector(top_similar),
    stringsAsFactors = FALSE
  ))
}

#' Get common industries between two geographies
#'
#' @param geoid1 First geography ID
#' @param geoid2 Second geography ID
#' @param M_matrix Binary specialization matrix
#' @param industry_names Data frame with industry codes and descriptions
#' @return Data frame with common industries
#'
get_common_industries <- function(geoid1, geoid2, M_matrix, industry_names = NULL) {
  if (!geoid1 %in% rownames(M_matrix) || !geoid2 %in% rownames(M_matrix)) {
    warning("One or both geographies not found in M_matrix")
    return(NULL)
  }
  
  common <- (M_matrix[geoid1, ] == 1) & (M_matrix[geoid2, ] == 1)
  common_industry_codes <- colnames(M_matrix)[common]
  
  result <- data.frame(industry_code = common_industry_codes, stringsAsFactors = FALSE)
  
  if (!is.null(industry_names)) {
    result <- result %>% left_join(industry_names, by = "industry_code")
  }
  return(result)
}

#' Compute peer geography analysis (similarity matrix and peer relationships)
#'
#' @param complexity_results Output from calculate_complexity_metrics()
#' @param geo_names Data frame with geoid and name columns
#' @param n_peers Number of peer geographies to find for each geography
#' @return List with similarity matrix and peer geography data
#'
compute_peer_geography_analysis <- function(complexity_results, geo_names, n_peers = 5) {
  
  M_matrix <- complexity_results$M_matrix
  n_geos <- nrow(M_matrix)
  
  # Calculate similarity matrix
  similarity_matrix <- calculate_geography_similarity(M_matrix)
  
  log_msg("  Similarity matrix: ", n_geos, " x ", n_geos, 
          " (range ", round(min(similarity_matrix), 3), "-", round(max(similarity_matrix), 3), ")\n")
  
  # Find peers for each geography
  geo_ids <- rownames(M_matrix)
  all_peers <- lapply(geo_ids, function(geoid) {
    peers <- get_similar_geographies(geoid, similarity_matrix, n_peers)
    if (!is.null(peers)) {
      peers$geoid <- geoid
      peers$peer_rank <- 1:nrow(peers)
    }
    return(peers)
  })
  
  peer_df <- bind_rows(all_peers) %>%
    select(geoid, peer_rank, peer_geoid, jaccard_similarity)
  
  # Add geography names
  peer_df <- peer_df %>%
    left_join(geo_names, by = "geoid") %>%
    rename(geography_name = name) %>%
    left_join(geo_names %>% rename(peer_geoid = geoid, peer_name = name), by = "peer_geoid")
  
  log_msg("  Peer relationships: ", format(nrow(peer_df), big.mark = ","), "\n")
  
  return(list(
    similarity_matrix = similarity_matrix,
    peer_relationships = peer_df
  ))
}


# =============================================================================
# RANKING PRINT FUNCTION
# =============================================================================

#' Print top 10 ECI geographies and ICI industries for a given complexity result
print_top10_rankings <- function(complexity_results, geo_names, industry_names, geo_level_name) {
  
  cat("\n  ", toupper(geo_level_name), " - Top 10 by ECI:\n", sep = "")
  top_eci <- complexity_results$eci %>%
    arrange(desc(economic_complexity_index)) %>%
    head(10) %>%
    left_join(geo_names, by = "geoid") %>%
    mutate(
      rank = row_number(),
      eci = round(economic_complexity_index, 3),
      display = paste0("    ", rank, ". ", name, " (", eci, ")")
    )
  cat(paste(top_eci$display, collapse = "\n"), "\n")
  
  cat("\n  ", toupper(geo_level_name), " - Top 10 Industries by ICI:\n", sep = "")
  top_ici <- complexity_results$ici %>%
    arrange(desc(industry_complexity_index)) %>%
    head(10) %>%
    left_join(industry_names, by = "industry_code") %>%
    mutate(
      rank = row_number(),
      ici = round(industry_complexity_index, 3),
      display = paste0("    ", rank, ". ", industry_description, " [", industry_code, "] (", ici, ")")
    )
  cat(paste(top_ici$display, collapse = "\n"), "\n")
}

# =============================================================================
# STEP 1: Get Alaska County Populations for Valdez-Cordova Split
# =============================================================================

log_step(1, "Processing Alaska Population Data")

alaska_pop <- getCensus(
  name = "acs/acs5",
  vintage = 2023,
  vars = c("NAME", "B01003_001E"),
  region = "county:*",
  regionin = "state:02"
) %>%
  mutate(
    county_geoid = paste0(state, county),
    population = as.numeric(B01003_001E)
  )

data_summary(alaska_pop, "Alaska counties", show_glimpse = TRUE)

chugach_share <- alaska_pop$population[alaska_pop$county_geoid == "02063"] /
  sum(alaska_pop$population[alaska_pop$county_geoid %in% c("02063", "02066")])
copper_river_share <- 1 - chugach_share

log_detail("  Chugach share:", round(chugach_share, 4), "| Copper River:", round(copper_river_share, 4), "\n")

# =============================================================================
# STEP 2: Load TIGRIS Geography Data
# =============================================================================

log_step(2, "Loading TIGRIS Geography Data")

tigris_2024_counties <- counties(cb = FALSE, year = 2024, class = "sf") %>%
  filter(!STATEFP %in% c("60", "66", "69", "72", "78")) %>%
  st_drop_geometry() %>%
  select(county_geoid = GEOID, county_name = NAMELSAD)

tigris_2024_states <- states(cb = TRUE, year = 2024, class = "sf") %>%
  filter(!STATEFP %in% c("60", "66", "69", "72", "78")) %>%
  st_drop_geometry() %>%
  select(state_fips = STATEFP, state_name = NAME)

data_summary(tigris_2024_counties, "Counties", show_glimpse = TRUE)
data_summary(tigris_2024_states, "States", show_glimpse = TRUE)

# =============================================================================
# STEP 3: Load and Process Lightcast Data
# =============================================================================

log_step(3, "Processing Lightcast Data")

lightcast_2024_data <- read_csv(lightcast_file, show_col_types = FALSE) %>%
  {
    bind_rows(
      filter(., COUNTY_ID != 2261),
      filter(., COUNTY_ID == 2261) %>%
        mutate(
          COUNTY_ID = 2063,
          EMPLOYMENT = round(EMPLOYMENT * chugach_share)
        ),
      filter(., COUNTY_ID == 2261) %>%
        mutate(
          COUNTY_ID = 2066,
          EMPLOYMENT = round(EMPLOYMENT * copper_river_share)
        )
    )
  } %>%
  mutate(county_geoid = str_pad(COUNTY_ID, width = 5, side = "left", pad = "0")) %>%
  rename(
    industry_code = IND_ID,
    industry_description = IND_DESCRIPTION,
    industry_employment_county = EMPLOYMENT
  ) %>%
  mutate(industry_code = as.character(industry_code)) %>%
  select(county_geoid, industry_code, industry_description, industry_employment_county) %>%
  {
    industry_lookup <- distinct(., industry_code, industry_description)
    complete(., county_geoid, industry_code, fill = list(industry_employment_county = 0)) %>%
      select(-industry_description) %>%
      left_join(industry_lookup, by = "industry_code")
  } %>%
  mutate(
    state_fips = str_sub(county_geoid, 1, 2),
    county_fips = str_sub(county_geoid, 3, 5),
    unknown_undefined_county = (county_fips == "999")
  ) %>%
  group_by(county_geoid) %>%
  mutate(total_employment_county = sum(industry_employment_county, na.rm = TRUE)) %>%
  ungroup() %>%
  group_by(industry_code) %>%
  mutate(industry_employment_nation = sum(industry_employment_county, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(total_employment_nation = sum(industry_employment_county[!duplicated(paste0(county_geoid, industry_code))], na.rm = TRUE)) %>%
  mutate(
    industry_employment_share_county = if_else(
      total_employment_county == 0, 0,
      industry_employment_county / total_employment_county
    ),
    industry_employment_share_nation = industry_employment_nation / total_employment_nation,
    industry_location_quotient_county = if_else(
      industry_employment_share_nation == 0 | total_employment_county == 0, 0,
      industry_employment_share_county / industry_employment_share_nation
    )
  ) %>%
  left_join(tigris_2024_counties, by = "county_geoid") %>%
  left_join(tigris_2024_states, by = "state_fips") %>%
  select(
    county_geoid, state_fips, county_fips, county_name, state_name, unknown_undefined_county,
    industry_code, industry_description,
    industry_employment_county, total_employment_county,
    industry_employment_nation, total_employment_nation,
    industry_employment_share_county, industry_employment_share_nation,
    industry_location_quotient_county
  )

data_summary(lightcast_2024_data, "Lightcast", show_glimpse = TRUE)
log_msg("  Counties:", n_distinct(lightcast_2024_data$county_geoid), 
        "| Industries:", n_distinct(lightcast_2024_data$industry_code), "\n")

# Stress test RCA calculation
log_debug("\n  --- STRESS TEST: RCA Calculation (Daboin Eq. 4) ---\n")
test_county <- lightcast_2024_data$county_geoid[1]
test_industry <- lightcast_2024_data$industry_code[1]
test_row <- lightcast_2024_data %>% 
  filter(county_geoid == test_county, industry_code == test_industry)

X_ci <- test_row$industry_employment_county
X_c <- test_row$total_employment_county
X_i <- test_row$industry_employment_nation
X <- test_row$total_employment_nation

manual_rca <- (X_ci / X_c) / (X_i / X)
computed_rca <- test_row$industry_location_quotient_county

stress_test("RCA manual verification (Eq. 4)",
            approx_equal(computed_rca, manual_rca),
            paste("Computed:", round(computed_rca, 6), "Manual:", round(manual_rca, 6)))

# =============================================================================
# STEP 4: Load Commuting Zone Data (2020 Commuting Zones from USDA ERS)
# =============================================================================

log_step(4, "Loading Commuting Zone Data")

cz_url <- "https://www.ers.usda.gov/media/6968/2020-commuting-zones.csv?v=56155"
log_detail("  Source:", cz_url, "\n")

cz <- readr::read_csv(cz_url, show_col_types = FALSE) %>%
  transmute(
    county_geoid = FIPStxt,
    commuting_zone_geoid = as.character(CZ2020),
    commuting_zone_name = CZName
  )

# Clean commuting zone names (remove suffixes like "city and borough", "CDP", etc.)
clean_cz_name <- function(x) {
  x <- stringr::str_squish(as.character(x))
  m <- stringr::str_match(x, "^(.*),\\s*(.+)$")
  place <- ifelse(is.na(m[,2]), x, m[,2])
  st <- ifelse(is.na(m[,3]), NA_character_, m[,3]) %>%
    stringr::str_replace_all("--", "-") %>%
    stringr::str_replace_all("\\s*-\\s*", "-") %>%
    stringr::str_squish()
  place <- place %>%
    stringr::str_replace(stringr::regex(
      "\\s+(city and borough|consolidated government \\(balance\\)|city|town|village|CDP)\\s*$", TRUE
    ), "") %>%
    stringr::str_squish()
  stringr::str_squish(stringr::str_replace(
    ifelse(is.na(st), place, paste0(place, ", ", st)), ",\\s+", ", "
  ))
}

cz <- cz %>%
  mutate(
    commuting_zone_name_original = commuting_zone_name,
    commuting_zone_name = clean_cz_name(commuting_zone_name)
  )

data_summary(cz, "Commuting zones", show_glimpse = TRUE)
log_msg("  Unique CZs:", n_distinct(cz$commuting_zone_geoid), "\n")

# =============================================================================
# STEP 5: Build County-State-CBSA-CSA-CZ Crosswalk
# =============================================================================

log_step(5, "Building Geographic Crosswalk")

counties_2024 <- counties(year = 2024, cb = FALSE) %>%
  filter(!STATEFP %in% c("60", "66", "69", "72", "78")) %>%
  transmute(
    state_fips = STATEFP,
    county_fips = COUNTYFP,
    county_geoid = GEOID,
    county_name = NAMELSAD,
    cbsa_geoid = if_else(is.na(CBSAFP) | CBSAFP == "", NA_character_, CBSAFP),
    csa_geoid = if_else(is.na(CSAFP) | CSAFP == "", NA_character_, CSAFP),
    geometry,
    county_in_cbsa = !is.na(CBSAFP) & CBSAFP != "",
    county_in_csa = !is.na(CSAFP) & CSAFP != ""
  )

states_2024 <- states(year = 2024, cb = FALSE) %>%
  st_set_geometry(NULL) %>%
  transmute(
    state_fips = STATEFP,
    state_abbreviation = STUSPS,
    state_name = NAME
  )

cbsa_2024 <- core_based_statistical_areas(year = 2024, cb = FALSE) %>%
  st_set_geometry(NULL) %>%
  transmute(cbsa_geoid = CBSAFP, cbsa_name = NAMELSAD)

csa_2024 <- combined_statistical_areas(year = 2024, cb = FALSE) %>%
  st_set_geometry(NULL) %>%
  transmute(csa_geoid = CSAFP, csa_name = NAMELSAD)

tigris_county_state_cbsa_csa_cz_2024 <- counties_2024 %>%
  left_join(states_2024, by = "state_fips") %>%
  left_join(cbsa_2024, by = "cbsa_geoid") %>%
  left_join(csa_2024, by = "csa_geoid") %>%
  left_join(cz %>% select(county_geoid, commuting_zone_geoid, commuting_zone_name), by = "county_geoid") %>%
  select(
    state_fips, state_name, state_abbreviation, county_fips, county_geoid, county_name,
    cbsa_geoid, cbsa_name, county_in_cbsa, csa_geoid, csa_name, county_in_csa,
    commuting_zone_geoid, commuting_zone_name, geometry
  )

county_state_cbsa_csa_cz_crosswalk <- tigris_county_state_cbsa_csa_cz_2024 %>% st_drop_geometry()
county_geometry <- tigris_county_state_cbsa_csa_cz_2024 %>% select(county_geoid, geometry) %>% st_as_sf()

data_summary(county_state_cbsa_csa_cz_crosswalk, "Crosswalk", show_glimpse = TRUE)
log_msg("  In CBSAs:", sum(county_state_cbsa_csa_cz_crosswalk$county_in_cbsa), 
        "| In CSAs:", sum(county_state_cbsa_csa_cz_crosswalk$county_in_csa), "\n")

# =============================================================================
# STEP 6: Create Lightcast with Full Geodata and Calculate All Metrics
# =============================================================================

log_step(6, "Calculating Employment Shares and LQs for All Geographic Levels")

lightcast_with_geodata <- lightcast_2024_data %>%
  left_join(
    county_state_cbsa_csa_cz_crosswalk %>%
      select(
        state_abbreviation, county_geoid, county_in_cbsa, cbsa_geoid, cbsa_name,
        county_in_csa, csa_geoid, csa_name, commuting_zone_geoid, commuting_zone_name
      ),
    by = "county_geoid"
  ) %>%
  mutate(
    county_name = if_else(unknown_undefined_county, "Unknown/Undefined", county_name),
    county_in_cbsa = if_else(unknown_undefined_county, FALSE, county_in_cbsa),
    county_in_csa = if_else(unknown_undefined_county, FALSE, county_in_csa)
  )

# State-level metrics
log_detail("  State metrics...\n")
state_totals <- lightcast_with_geodata %>%
  group_by(state_fips) %>%
  summarise(total_employment_state = sum(industry_employment_county, na.rm = TRUE), .groups = "drop")

state_industry <- lightcast_with_geodata %>%
  group_by(state_fips, industry_code) %>%
  summarise(industry_employment_state = sum(industry_employment_county, na.rm = TRUE), .groups = "drop") %>%
  left_join(state_totals, by = "state_fips")

national_shares <- lightcast_with_geodata %>%
  select(industry_code, industry_employment_share_nation) %>%
  distinct()

state_industry <- state_industry %>%
  left_join(national_shares, by = "industry_code") %>%
  mutate(
    industry_employment_share_state = if_else(total_employment_state == 0, 0,
                                              industry_employment_state / total_employment_state),
    industry_location_quotient_state = if_else(industry_employment_share_nation == 0 | total_employment_state == 0, 0,
                                               industry_employment_share_state / industry_employment_share_nation)
  )

data_summary(state_industry, "State-industry", show_glimpse = TRUE)

# CBSA-level metrics
log_detail("  Calculating CBSA-level metrics...\n")
cbsa_totals <- lightcast_with_geodata %>%
  filter(!is.na(cbsa_geoid)) %>%
  group_by(cbsa_geoid) %>%
  summarise(total_employment_cbsa = sum(industry_employment_county, na.rm = TRUE), .groups = "drop")

cbsa_industry <- lightcast_with_geodata %>%
  filter(!is.na(cbsa_geoid)) %>%
  group_by(cbsa_geoid, industry_code) %>%
  summarise(industry_employment_cbsa = sum(industry_employment_county, na.rm = TRUE), .groups = "drop") %>%
  left_join(cbsa_totals, by = "cbsa_geoid") %>%
  left_join(national_shares, by = "industry_code") %>%
  mutate(
    industry_employment_share_cbsa = if_else(total_employment_cbsa == 0, 0,
                                             industry_employment_cbsa / total_employment_cbsa),
    industry_location_quotient_cbsa = if_else(industry_employment_share_nation == 0 | total_employment_cbsa == 0, 0,
                                              industry_employment_share_cbsa / industry_employment_share_nation)
  ) %>%
  select(-industry_employment_share_nation)

data_summary(cbsa_industry, "CBSA-industry", show_glimpse = TRUE)

# CSA-level metrics
log_detail("  Calculating CSA-level metrics...\n")
csa_totals <- lightcast_with_geodata %>%
  filter(!is.na(csa_geoid)) %>%
  group_by(csa_geoid) %>%
  summarise(total_employment_csa = sum(industry_employment_county, na.rm = TRUE), .groups = "drop")

csa_industry <- lightcast_with_geodata %>%
  filter(!is.na(csa_geoid)) %>%
  group_by(csa_geoid, industry_code) %>%
  summarise(industry_employment_csa = sum(industry_employment_county, na.rm = TRUE), .groups = "drop") %>%
  left_join(csa_totals, by = "csa_geoid") %>%
  left_join(national_shares, by = "industry_code") %>%
  mutate(
    industry_employment_share_csa = if_else(total_employment_csa == 0, 0,
                                            industry_employment_csa / total_employment_csa),
    industry_location_quotient_csa = if_else(industry_employment_share_nation == 0 | total_employment_csa == 0, 0,
                                             industry_employment_share_csa / industry_employment_share_nation)
  ) %>%
  select(-industry_employment_share_nation)

data_summary(csa_industry, "CSA-industry", show_glimpse = TRUE)

# Commuting zone-level metrics
log_detail("  Calculating commuting zone-level metrics...\n")
cz_totals <- lightcast_with_geodata %>%
  filter(!is.na(commuting_zone_geoid)) %>%
  group_by(commuting_zone_geoid) %>%
  summarise(total_employment_cz = sum(industry_employment_county, na.rm = TRUE), .groups = "drop")

cz_industry <- lightcast_with_geodata %>%
  filter(!is.na(commuting_zone_geoid)) %>%
  group_by(commuting_zone_geoid, industry_code) %>%
  summarise(industry_employment_cz = sum(industry_employment_county, na.rm = TRUE), .groups = "drop") %>%
  left_join(cz_totals, by = "commuting_zone_geoid") %>%
  left_join(national_shares, by = "industry_code") %>%
  mutate(
    industry_employment_share_cz = if_else(total_employment_cz == 0, 0,
                                           industry_employment_cz / total_employment_cz),
    industry_location_quotient_cz = if_else(industry_employment_share_nation == 0 | total_employment_cz == 0, 0,
                                            industry_employment_share_cz / industry_employment_share_nation)
  ) %>%
  select(-industry_employment_share_nation)

data_summary(cz_industry, "CZ-industry", show_glimpse = TRUE)

# Join all levels back together
lightcast_with_geodata <- lightcast_with_geodata %>%
  left_join(
    state_industry %>% select(
      state_fips, industry_code, industry_employment_state, total_employment_state,
      industry_employment_share_state, industry_location_quotient_state
    ),
    by = c("state_fips", "industry_code")
  ) %>%
  left_join(
    cbsa_industry %>% select(
      cbsa_geoid, industry_code, industry_employment_cbsa, total_employment_cbsa,
      industry_employment_share_cbsa, industry_location_quotient_cbsa
    ),
    by = c("cbsa_geoid", "industry_code")
  ) %>%
  left_join(
    csa_industry %>% select(
      csa_geoid, industry_code, industry_employment_csa, total_employment_csa,
      industry_employment_share_csa, industry_location_quotient_csa
    ),
    by = c("csa_geoid", "industry_code")
  ) %>%
  left_join(
    cz_industry %>% select(
      commuting_zone_geoid, industry_code, industry_employment_cz, total_employment_cz,
      industry_employment_share_cz, industry_location_quotient_cz
    ),
    by = c("commuting_zone_geoid", "industry_code")
  ) %>%
  rename(
    industry_employment_commuting_zone = industry_employment_cz,
    total_employment_commuting_zone = total_employment_cz,
    industry_employment_share_commuting_zone = industry_employment_share_cz,
    industry_location_quotient_commuting_zone = industry_location_quotient_cz
  ) %>%
  filter(!unknown_undefined_county) %>%
  select(-unknown_undefined_county) %>%
  select(
    state_fips, state_name, state_abbreviation,
    county_fips, county_geoid, county_name,
    county_in_cbsa, cbsa_geoid, cbsa_name,
    county_in_csa, csa_geoid, csa_name,
    commuting_zone_geoid, commuting_zone_name,
    industry_code, industry_description,
    industry_employment_county, total_employment_county,
    industry_employment_share_county, industry_location_quotient_county,
    industry_employment_state, total_employment_state,
    industry_employment_share_state, industry_location_quotient_state,
    industry_employment_cbsa, total_employment_cbsa,
    industry_employment_share_cbsa, industry_location_quotient_cbsa,
    industry_employment_csa, total_employment_csa,
    industry_employment_share_csa, industry_location_quotient_csa,
    industry_employment_commuting_zone, total_employment_commuting_zone,
    industry_employment_share_commuting_zone, industry_location_quotient_commuting_zone,
    industry_employment_nation, industry_employment_share_nation, total_employment_nation
  )

log_msg("  Final dataset:", nrow(lightcast_with_geodata), "rows\n")

log_detail("\nFinal lightcast data:\n")
data_summary(lightcast_with_geodata, show_glimpse = TRUE)

# =============================================================================
# STEP 7: Create Individual Employment Share/LQ Data Frames
# =============================================================================

log_step(7, "Creating EmpShare/LQ Data Frames for Each Geographic Level")


county_empshare_lq <- lightcast_with_geodata %>%
  select(county_geoid, industry_code,
         industry_employment_share_county, industry_location_quotient_county) %>%
  distinct()

state_empshare_lq <- lightcast_with_geodata %>%
  select(state_fips, industry_code,
         industry_employment_share_state, industry_location_quotient_state) %>%
  distinct()

cbsa_empshare_lq <- lightcast_with_geodata %>%
  filter(!is.na(cbsa_geoid)) %>%
  select(cbsa_geoid, industry_code,
         industry_employment_share_cbsa, industry_location_quotient_cbsa) %>%
  distinct()

csa_empshare_lq <- lightcast_with_geodata %>%
  filter(!is.na(csa_geoid)) %>%
  select(csa_geoid, industry_code,
         industry_employment_share_csa, industry_location_quotient_csa) %>%
  distinct()

commuting_zone_empshare_lq <- lightcast_with_geodata %>%
  filter(!is.na(commuting_zone_geoid)) %>%
  select(commuting_zone_geoid, industry_code,
         industry_employment_share_commuting_zone, industry_location_quotient_commuting_zone) %>%
  distinct()

log_msg("  County:", nrow(county_empshare_lq), "\n")
log_msg("  State:", nrow(state_empshare_lq), "\n")
log_msg("  CBSA:", nrow(cbsa_empshare_lq), "\n")
log_msg("  CSA:", nrow(csa_empshare_lq), "\n")
log_msg("  CZ:", nrow(commuting_zone_empshare_lq), "\n")

log_detail("\nCounty empshare/LQ:\n")
data_summary(county_empshare_lq, show_glimpse = TRUE)

# =============================================================================
# STEP 8: Create Combined EmpShare/LQ Data Frame
# =============================================================================

log_step(8, "Creating Combined EmpShare/LQ Frame")


geo_aggregation_levels <- tibble(
  geo_aggregation_level = c(1L, 2L, 3L, 4L, 5L),
  geo_aggregation_name = c("county", "state", "cbsa", "csa", "commuting_zone")
)

industry_titles <- lightcast_with_geodata %>%
  select(industry_code, industry_description) %>%
  distinct() %>%
  arrange(industry_code)

combined_empshare_lq <- bind_rows(
  county_empshare_lq %>%
    transmute(geoid = county_geoid, geo_aggregation_level = 1L, industry_code,
              industry_empshare = industry_employment_share_county,
              industry_lq = industry_location_quotient_county),
  state_empshare_lq %>%
    transmute(geoid = state_fips, geo_aggregation_level = 2L, industry_code,
              industry_empshare = industry_employment_share_state,
              industry_lq = industry_location_quotient_state),
  cbsa_empshare_lq %>%
    transmute(geoid = cbsa_geoid, geo_aggregation_level = 3L, industry_code,
              industry_empshare = industry_employment_share_cbsa,
              industry_lq = industry_location_quotient_cbsa),
  csa_empshare_lq %>%
    transmute(geoid = csa_geoid, geo_aggregation_level = 4L, industry_code,
              industry_empshare = industry_employment_share_csa,
              industry_lq = industry_location_quotient_csa),
  commuting_zone_empshare_lq %>%
    transmute(geoid = commuting_zone_geoid, geo_aggregation_level = 5L, industry_code,
              industry_empshare = industry_employment_share_commuting_zone,
              industry_lq = industry_location_quotient_commuting_zone)
)

log_msg("  Combined:", nrow(combined_empshare_lq), "\n")

log_detail("\nCombined empshare/LQ:\n")
data_summary(combined_empshare_lq, show_glimpse = TRUE)

# =============================================================================
# STEP 9: Calculate Complexity Metrics for Each Geographic Level
# =============================================================================

log_step(9, "Calculating Complexity Metrics (All Levels)")

county_names <- lightcast_with_geodata %>% select(geoid = county_geoid, name = county_name) %>% distinct()
state_names  <- lightcast_with_geodata %>% select(geoid = state_fips,  name = state_name)  %>% distinct()
cbsa_names   <- lightcast_with_geodata %>% filter(!is.na(cbsa_geoid)) %>% select(geoid = cbsa_geoid, name = cbsa_name) %>% distinct()
csa_names    <- lightcast_with_geodata %>% filter(!is.na(csa_geoid))  %>% select(geoid = csa_geoid,  name = csa_name)  %>% distinct()
cz_names     <- lightcast_with_geodata %>% filter(!is.na(commuting_zone_geoid)) %>%
  select(geoid = commuting_zone_geoid, name = commuting_zone_name) %>% distinct()

# 9.1 County
county_data_for_complexity <- combined_empshare_lq %>%
  filter(geo_aggregation_level == 1L) %>%
  select(geoid, industry_code, industry_lq)
county_complexity <- calculate_complexity_metrics(county_data_for_complexity, "county",
                                                  run_stress_tests = TRUE,
                                                  create_industry_space = TRUE)
print_top10_rankings(county_complexity, county_names, industry_titles, "county")

# 9.2 State
state_data_for_complexity <- combined_empshare_lq %>%
  filter(geo_aggregation_level == 2L) %>%
  select(geoid, industry_code, industry_lq)
state_complexity <- calculate_complexity_metrics(state_data_for_complexity, "state",
                                                 run_stress_tests = TRUE,
                                                 create_industry_space = TRUE)
print_top10_rankings(state_complexity, state_names, industry_titles, "state")

# 9.3 CBSA
cbsa_data_for_complexity <- combined_empshare_lq %>%
  filter(geo_aggregation_level == 3L) %>%
  select(geoid, industry_code, industry_lq)
cbsa_complexity <- calculate_complexity_metrics(cbsa_data_for_complexity, "cbsa",
                                                run_stress_tests = TRUE,
                                                create_industry_space = TRUE)
print_top10_rankings(cbsa_complexity, cbsa_names, industry_titles, "cbsa")

# 9.4 CSA
csa_data_for_complexity <- combined_empshare_lq %>%
  filter(geo_aggregation_level == 4L) %>%
  select(geoid, industry_code, industry_lq)
csa_complexity <- calculate_complexity_metrics(csa_data_for_complexity, "csa",
                                               run_stress_tests = TRUE,
                                               create_industry_space = TRUE)
print_top10_rankings(csa_complexity, csa_names, industry_titles, "csa")

# 9.5 Commuting Zone
cz_data_for_complexity <- combined_empshare_lq %>%
  filter(geo_aggregation_level == 5L) %>%
  select(geoid, industry_code, industry_lq)
cz_complexity <- calculate_complexity_metrics(cz_data_for_complexity, "commuting_zone",
                                              run_stress_tests = TRUE,
                                              create_industry_space = TRUE)
print_top10_rankings(cz_complexity, cz_names, industry_titles, "commuting zone")

# =============================================================================
# STEP 10: Create Industry Space Visualizations (OPTIMIZED)
# =============================================================================

log_step(10, "Creating Industry Space Visualizations")


# Create industry space plots for each level (now uses vectorized edge creation)
county_industry_space <- create_industry_space_plot(county_complexity, industry_titles, "County")
cbsa_industry_space <- create_industry_space_plot(cbsa_complexity, industry_titles, "CBSA")
cz_industry_space <- create_industry_space_plot(cz_complexity, industry_titles, "Commuting Zone")

# Display one plot (CBSA is typically most interpretable)
if (!is.null(cbsa_industry_space)) {
  print(cbsa_industry_space)
}


# =============================================================================
# STEP 11: Peer Geography Analysis
# =============================================================================

log_step(11, "Peer Geography Analysis")

# Compute peer similarity matrices based on M_ci (Jaccard similarity)
log_msg("  Computing county peers...\n")
county_peers <- compute_peer_geography_analysis(county_complexity, county_names, n_peers = 5)

log_msg("  Computing CBSA peers...\n")
cbsa_peers <- compute_peer_geography_analysis(cbsa_complexity, cbsa_names, n_peers = 5)

# =============================================================================
# STEP 12: Create Combined Complexity Data Frames
# =============================================================================

log_step(12, "Creating Combined Complexity Data Frames")

combined_diversity <- bind_rows(
  county_complexity$diversity %>% mutate(geo_aggregation_level = 1L),
  state_complexity$diversity %>% mutate(geo_aggregation_level = 2L),
  cbsa_complexity$diversity %>% mutate(geo_aggregation_level = 3L),
  csa_complexity$diversity %>% mutate(geo_aggregation_level = 4L),
  cz_complexity$diversity %>% mutate(geo_aggregation_level = 5L)
)

combined_ubiquity <- bind_rows(
  county_complexity$ubiquity %>% mutate(geo_aggregation_level = 1L),
  state_complexity$ubiquity %>% mutate(geo_aggregation_level = 2L),
  cbsa_complexity$ubiquity %>% mutate(geo_aggregation_level = 3L),
  csa_complexity$ubiquity %>% mutate(geo_aggregation_level = 4L),
  cz_complexity$ubiquity %>% mutate(geo_aggregation_level = 5L)
)

combined_eci <- bind_rows(
  county_complexity$eci %>% mutate(geo_aggregation_level = 1L),
  state_complexity$eci %>% mutate(geo_aggregation_level = 2L),
  cbsa_complexity$eci %>% mutate(geo_aggregation_level = 3L),
  csa_complexity$eci %>% mutate(geo_aggregation_level = 4L),
  cz_complexity$eci %>% mutate(geo_aggregation_level = 5L)
)

combined_ici <- bind_rows(
  county_complexity$ici %>% mutate(geo_aggregation_level = 1L),
  state_complexity$ici %>% mutate(geo_aggregation_level = 2L),
  cbsa_complexity$ici %>% mutate(geo_aggregation_level = 3L),
  csa_complexity$ici %>% mutate(geo_aggregation_level = 4L),
  cz_complexity$ici %>% mutate(geo_aggregation_level = 5L)
)

log_msg("  Combined: diversity=", nrow(combined_diversity), " ubiquity=", nrow(combined_ubiquity),
        " ECI=", nrow(combined_eci), " ICI=", nrow(combined_ici), "\n")

data_summary(combined_eci, "Combined ECI", show_glimpse = TRUE)
data_summary(combined_ici, "Combined ICI", show_glimpse = TRUE)

# =============================================================================
# STEP 13: Final Validation Summary
# =============================================================================

log_step(13, "Final Validation Summary")

log_msg("Coverage: ", n_distinct(lightcast_with_geodata$county_geoid), " counties, ",
        n_distinct(lightcast_with_geodata$state_fips), " states, ",
        n_distinct(lightcast_with_geodata$cbsa_geoid, na.rm = TRUE), " CBSAs, ",
        n_distinct(lightcast_with_geodata$industry_code), " industries\n")

# Compact validation table
validation_results <- tibble(
  Level = c("County", "State", "CBSA", "CSA", "CZ"),
  `λ1=1` = c(
    ifelse(abs(county_complexity$diagnostics$first_eigenvalue_C - 1) < 1e-6, "✓", "✗"),
    ifelse(abs(state_complexity$diagnostics$first_eigenvalue_C - 1) < 1e-6, "✓", "✗"),
    ifelse(abs(cbsa_complexity$diagnostics$first_eigenvalue_C - 1) < 1e-6, "✓", "✗"),
    ifelse(abs(csa_complexity$diagnostics$first_eigenvalue_C - 1) < 1e-6, "✓", "✗"),
    ifelse(abs(cz_complexity$diagnostics$first_eigenvalue_C - 1) < 1e-6, "✓", "✗")
  ),
  `ICI-Ubiq<0` = c(
    ifelse(county_complexity$diagnostics$ici_ubiquity_correlation < 0, "✓", "✗"),
    ifelse(state_complexity$diagnostics$ici_ubiquity_correlation < 0, "✓", "✗"),
    ifelse(cbsa_complexity$diagnostics$ici_ubiquity_correlation < 0, "✓", "✗"),
    ifelse(csa_complexity$diagnostics$ici_ubiquity_correlation < 0, "✓", "✗"),
    ifelse(cz_complexity$diagnostics$ici_ubiquity_correlation < 0, "✓", "✗")
  ),
  `|ECI-avgICI|≈1` = c(
    ifelse(abs(county_complexity$diagnostics$eci_avgici_correlation) > 0.99, "✓", "✗"),
    ifelse(abs(state_complexity$diagnostics$eci_avgici_correlation) > 0.99, "✓", "✗"),
    ifelse(abs(cbsa_complexity$diagnostics$eci_avgici_correlation) > 0.99, "✓", "✗"),
    ifelse(abs(csa_complexity$diagnostics$eci_avgici_correlation) > 0.99, "✓", "✗"),
    ifelse(abs(cz_complexity$diagnostics$eci_avgici_correlation) > 0.99, "✓", "✗")
  ),
  λ2 = round(c(
    county_complexity$diagnostics$second_eigenvalue_C,
    state_complexity$diagnostics$second_eigenvalue_C,
    cbsa_complexity$diagnostics$second_eigenvalue_C,
    csa_complexity$diagnostics$second_eigenvalue_C,
    cz_complexity$diagnostics$second_eigenvalue_C
  ), 4),
  Fill = paste0(round(c(
    county_complexity$diagnostics$fill_rate,
    state_complexity$diagnostics$fill_rate,
    cbsa_complexity$diagnostics$fill_rate,
    csa_complexity$diagnostics$fill_rate,
    cz_complexity$diagnostics$fill_rate
  ), 1), "%")
)

print(validation_results, n = 5)

# =============================================================================
# DATA FRAME INVENTORY
# =============================================================================

cat("\n================================================================================\n")
cat("DATA FRAMES CREATED\n")
cat("================================================================================\n")

# List all data frames in the environment with their dimensions
data_frame_inventory <- tibble(
  Object = c(
    # Geographic reference data
    "alaska_pop", "tigris_2024_counties", "tigris_2024_states", "cz",
    "county_state_cbsa_csa_cz_crosswalk", "county_geometry",
    
    # Employment data
    "lightcast_2024_data", "lightcast_with_geodata",
    "state_industry", "cbsa_industry", "csa_industry", "cz_industry",
    
    # Empshare/LQ data frames
    "county_empshare_lq", "state_empshare_lq", "cbsa_empshare_lq", 
    "csa_empshare_lq", "commuting_zone_empshare_lq", "combined_empshare_lq",
    
    # Geographic name lookups
    "county_names", "state_names", "cbsa_names", "csa_names", "cz_names",
    "industry_titles", "geo_aggregation_levels",
    
    # Combined complexity outputs
    "combined_diversity", "combined_ubiquity", "combined_eci", "combined_ici",
    
    # Validation
    "validation_results"
  ),
  Description = c(
    # Geographic reference data
    "Alaska county populations (for Valdez-Cordova split)",
    "TIGRIS county reference (geoid, name)",
    "TIGRIS state reference (fips, name)",
    "Commuting zone crosswalk (county to CZ)",
    "Full county-state-CBSA-CSA-CZ crosswalk",
    "County geometry (sf object)",
    
    # Employment data
    "Raw Lightcast data (with Alaska fix)",
    "Lightcast with all geographic levels joined",
    "State-industry employment and LQs",
    "CBSA-industry employment and LQs",
    "CSA-industry employment and LQs",
    "Commuting zone-industry employment and LQs",
    
    # Empshare/LQ data frames
    "County employment shares and LQs",
    "State employment shares and LQs",
    "CBSA employment shares and LQs",
    "CSA employment shares and LQs",
    "Commuting zone employment shares and LQs",
    "All levels combined (long format)",
    
    # Geographic name lookups
    "County geoid-to-name lookup",
    "State fips-to-name lookup",
    "CBSA geoid-to-name lookup",
    "CSA geoid-to-name lookup",
    "Commuting zone geoid-to-name lookup",
    "Industry code-to-description lookup",
    "Geographic aggregation level codes",
    
    # Combined complexity outputs
    "Diversity (K_c0) for all levels",
    "Ubiquity (K_i0) for all levels",
    "ECI for all levels",
    "ICI for all levels",
    
    # Validation
    "Daboin equation validation summary"
  )
)

# Add dimensions
data_frame_inventory <- data_frame_inventory %>%
  rowwise() %>%
  mutate(
    Rows = if (exists(Object)) nrow(get(Object)) else NA_integer_,
    Cols = if (exists(Object)) ncol(get(Object)) else NA_integer_
  ) %>%
  ungroup()

print(data_frame_inventory, n = 50)

cat("\n================================================================================\n")
cat("COMPLEXITY RESULT OBJECTS (lists with multiple components)\n")
cat("================================================================================\n")

complexity_objects <- tibble(
  Object = c("county_complexity", "state_complexity", "cbsa_complexity", 
             "csa_complexity", "cz_complexity"),
  Level = c("County", "State", "CBSA", "CSA", "Commuting Zone"),
  Components = "eci, ici, diversity, ubiquity, K_c1, K_i1, M_matrix, M_tilde_C, M_tilde_I, proximity, diagnostics"
)
print(complexity_objects)

cat("\nPEER GEOGRAPHY OBJECTS:\n")
peer_objects <- tibble(
  Object = c("county_peers", "cbsa_peers"),
  Level = c("County", "CBSA"),
  Components = "similarity_matrix, peer_relationships"
)
print(peer_objects, n = 2)

cat("\n================================================================================\n")
cat("DATA FRAME GLIMPSES\n")
cat("================================================================================\n")

for (obj_name in data_frame_inventory$Object) {
  if (exists(obj_name)) {
    cat("\n---", obj_name, "---\n")
    glimpse(get(obj_name))
  }
}

cat("\n================================================================================\n")
cat("✓ SCRIPT COMPLETE\n")
cat("================================================================================\n")