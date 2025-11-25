# BESS Pro Files Workflow Functions
# 
# These functions handle BESS-formatted PRO files which have different header
# structures and metadata organization compared to SIO-formatted files.
# BESS format uses % comment headers with specific patterns for tow information,
# dates, and instrument details.

library(dplyr)
library(purrr)
library(readr)
library(lubridate)
library(stringr)
library(rlang)

#' Extract metadata from BESS-formatted PRO file headers
#'
#' Parses BESS PRO file headers to extract tow information, dates, vessel info,
#' and instrument details. BESS format uses % comment lines with specific patterns.
#'
#' @param file_path Character. Path to BESS-formatted PRO file
#' @return List containing extracted metadata
#' @details
#' BESS format structure:
#' - % Tow: [tow_id]   [vessel_info]
#' - % Date: [date_string]  
#' - % [Instrument] Probe # [number]
#' - % time    pres  echo ... (column headers)
#' 
#' @examples
#' \dontrun{
#' metadata <- extract_bess_pro_metadata("M-01-005.PRO")
#' }
#' @export
extract_bess_pro_metadata <- function(file_path) {
  
  # Read first 20 lines to capture all header information
  header_lines <- readLines(file_path, n = 20) |>
    str_trim()
  
  # Extract comment lines (start with %)
  comment_lines <- header_lines[str_detect(header_lines, "^%")]
  
  # Initialize metadata list
  metadata <- list(
    file_path = file_path,
    file_name = basename(file_path),
    tow_id = NA_character_,
    vessel = NA_character_,
    date_string = NA_character_,
    parsed_date = NA_real_,
    instrument_probes = list(),
    flow_meter_calibration = NA_real_,
    column_headers = character(),
    num_columns = NA_integer_
  )
  
  # Extract tow information and vessel
  tow_line <- comment_lines[str_detect(comment_lines, "^% Tow:")]
  if (length(tow_line) > 0) {
    tow_match <- str_match(tow_line, "^% Tow:\\s*([^\\s]+)\\s+(.+)$")
    if (!is.na(tow_match[1,2])) {
      metadata$tow_id <- str_trim(tow_match[1,2])
    }
    if (!is.na(tow_match[1,3])) {
      metadata$vessel <- str_trim(tow_match[1,3])
    }
  }
  
  # Extract date
  date_line <- comment_lines[str_detect(comment_lines, "^% Date:")]
  if (length(date_line) > 0) {
    date_match <- str_match(date_line, "^% Date:\\s*(.+)$")
    if (!is.na(date_match[1,2])) {
      metadata$date_string <- str_trim(date_match[1,2])
      
      # Parse date - BESS format: "13 Aug 2011" or "2 Sept 2012"
      # First, normalize "Sept" to "Sep" for proper parsing
      normalized_date <- str_replace(metadata$date_string, "Sept", "Sep")
      
      parsed_date <- tryCatch({
        dmy(normalized_date)
      }, error = function(e) {
        # Try alternative parsing if first attempt fails
        tryCatch({
          parse_date_time(normalized_date, orders = c("dmy", "dmY", "dby", "dbY"))
        }, error = function(e2) NA_real_)
      })
      
      if (!is.na(parsed_date)) {
        metadata$parsed_date <- as.numeric(as.Date(parsed_date))
      }
    }
  }
  
  # Extract instrument probe information
  probe_lines <- comment_lines[str_detect(comment_lines, "Probe #")]
  if (length(probe_lines) > 0) {
    probe_info <- map(probe_lines, function(line) {
      # Split on multiple instruments in one line
      # Pattern: "% Temperature Probe #  5164    Conductivity Probe #  3613"
      instruments <- str_split(line, "(?<=\\d)\\s+(?=[A-Z])")[[1]]
      
      map(instruments, function(inst_part) {
        probe_match <- str_match(inst_part, "([A-Z][a-z]+)\\s+Probe\\s*#\\s*(\\d+)")
        if (!is.na(probe_match[1,2])) {
          list(
            instrument = str_trim(probe_match[1,2]),
            probe_number = as.numeric(probe_match[1,3])
          )
        } else {
          NULL
        }
      })
    }) |>
    list_flatten() |>
    compact()
    
    metadata$instrument_probes <- probe_info
  }
  
  # Extract flow meter calibration
  flow_line <- comment_lines[str_detect(comment_lines, "Flow Meter Calibration")]
  if (length(flow_line) > 0) {
    flow_match <- str_match(flow_line, "([0-9.]+)\\s*\\(m/count\\)")
    if (!is.na(flow_match[1,2])) {
      metadata$flow_meter_calibration <- as.numeric(flow_match[1,2])
    }
  }
  
  # Extract column headers
  column_line <- comment_lines[str_detect(comment_lines, "^%\\s*(time\\s+pres|time\\s.*pres)")]
  if (length(column_line) > 0) {
    # Remove leading % and extra whitespace, then split on whitespace
    headers_clean <- str_remove(column_line[1], "^%\\s*") |>
      str_trim()
    
    # Split on one or more whitespace characters
    column_names <- str_split(headers_clean, "\\s+")[[1]]
    metadata$column_headers <- column_names
    metadata$num_columns <- length(column_names)
  }
  
  return(metadata)
}


#' Ingest single BESS-formatted PRO file
#'
#' Reads and processes a single BESS PRO file, extracting metadata and data.
#' Handles the BESS-specific header format and data structure.
#'
#' @param file_path Character. Path to BESS-formatted PRO file
#' @return Tibble with processed PRO data and metadata
#' @export
ingest_bess_pro_file <- function(file_path) {
  
  # Extract metadata first
  metadata <- extract_bess_pro_metadata(file_path)
  
  # Determine data start line (first non-comment line after headers)
  all_lines <- readLines(file_path)
  comment_mask <- str_detect(all_lines, "^%")
  data_start <- which(!comment_mask)[1]
  
  if (is.na(data_start)) {
    rlang::abort(paste("No data found in file:", file_path))
  }
  
  # Read data portion with appropriate column names
  col_names <- metadata$column_headers
  if (length(col_names) == 0) {
    # Default column names if headers not found
    col_names <- c("time", "pres", "echo", "temp", "theta", "sal", "sigma", 
                   "angle", "flow", "hzvel", "vtvel", "vol", "net", "fluor", 
                   "ptran", "oxygen", "Irradcurrent", "Irradiance", "lat", "lon")
  }
  
  # Read data using readr
  pro_data <- tryCatch({
    read_table(file_path, 
               skip = data_start - 1,
               col_names = col_names,
               col_types = cols(.default = col_double()),
               show_col_types = FALSE)
  }, error = function(e) {
    rlang::abort(paste("Failed to read data from file:", file_path, "Error:", e$message))
  })
  
  # Add metadata columns
  pro_data <- pro_data |>
    mutate(
      file_name = metadata$file_name,
      tow_id = metadata$tow_id,
      vessel = metadata$vessel,
      date_string = metadata$date_string,
      parsed_date = metadata$parsed_date,
      .before = 1
    )
  
  # Convert decimal time to datetime if possible
  if ("time" %in% names(pro_data) && !is.na(metadata$parsed_date)) {
    base_date <- as.Date(metadata$parsed_date, origin = "1970-01-01")
    
    pro_data <- pro_data |>
      mutate(
        decimal_time = time,
        datetime_gmt = base_date + days(floor(time)) + 
                      seconds((time - floor(time)) * 24 * 60 * 60),
        .after = time
      )
  }
  
  return(pro_data)
}


#' Load and process multiple BESS-formatted PRO files
#'
#' Batch processes BESS PRO files from specified directory or file list.
#' Includes parallel processing support and comprehensive error handling.
#'
#' @param file_paths Character vector of file paths, or directory path
#' @param pattern Character. File pattern to match if file_paths is directory
#' @param parallel Logical. Use parallel processing (default: FALSE)
#' @param add_daynight Logical. Add day/night annotation (default: TRUE)
#' @param progress Logical. Show progress bar (default: TRUE)
#' @return Tibble with combined data from all processed files
#' @export
load_bess_pro_files <- function(file_paths, 
                                pattern = "\\.(PRO|pro)$",
                                parallel = FALSE,
                                add_daynight = TRUE,
                                progress = TRUE) {
  
  # Handle directory input
  if (length(file_paths) == 1 && dir.exists(file_paths)) {
    file_paths <- list.files(file_paths, pattern = pattern, full.names = TRUE)
  }
  
  # Validate files exist
  missing_files <- file_paths[!file.exists(file_paths)]
  if (length(missing_files) > 0) {
    rlang::abort(paste("Files not found:", paste(missing_files, collapse = ", ")))
  }
  
  if (length(file_paths) == 0) {
    rlang::abort("No files found to process")
  }
  
  # Set up processing function
  process_file <- function(file_path) {
    tryCatch({
      ingest_bess_pro_file(file_path)
    }, error = function(e) {
      rlang::warn(paste("Failed to process file", file_path, ":", e$message))
      NULL
    })
  }
  
  # Process files (parallel or sequential)
  if (parallel) {
    # Use future/furrr for parallel processing if available
    if (requireNamespace("furrr", quietly = TRUE) && 
        requireNamespace("future", quietly = TRUE)) {
      
      if (progress) {
        message("Processing ", length(file_paths), " files in parallel...")
      }
      
      results <- furrr::future_map(file_paths, process_file, 
                                   .progress = progress,
                                   .options = furrr::furrr_options(seed = TRUE))
    } else {
      rlang::warn("Parallel processing packages not available. Using sequential processing.")
      results <- map(file_paths, process_file, .progress = progress)
    }
  } else {
    if (progress) {
      message("Processing ", length(file_paths), " files sequentially...")
    }
    results <- map(file_paths, process_file, .progress = progress)
  }
  
  # Remove failed files and combine results
  successful_results <- compact(results)
  
  if (length(successful_results) == 0) {
    rlang::abort("No files were successfully processed")
  }
  
  if (length(successful_results) < length(file_paths)) {
    n_failed <- length(file_paths) - length(successful_results)
    rlang::warn(paste(n_failed, "files failed to process"))
  }
  
  # Combine all data
  combined_data <- bind_rows(successful_results)
  
  # Add day/night annotation if requested and possible
  if (add_daynight && 
      all(c("lat", "lon", "datetime_gmt") %in% names(combined_data))) {
    
    if (progress) {
      message("Adding day/night annotations...")
    }
    
    # Use the existing annotate_daytime function
    combined_data <- annotate_daytime(combined_data)
  }
  
  if (progress) {
    message("Successfully processed ", length(successful_results), " files with ", 
            nrow(combined_data), " total observations")
  }
  
  return(combined_data)
}