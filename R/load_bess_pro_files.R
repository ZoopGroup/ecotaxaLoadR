# BESS Pro Files Workflow Functions
# 
# These functions handle BESS-formatted PRO files which have different header
# structures and metadata organization compared to SIO-formatted files.
# BESS format uses % comment headers with specific patterns for tow information,
# dates, and instrument details.

# library(dplyr)
# library(purrr)
# library(readr)
# library(lubridate)
# library(stringr)
# library(rlang)

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
  header_lines <- readLines(file_path, n = 20)
  header_lines <- stringr::str_trim(header_lines)
  
  # Extract comment lines (start with %)
  comment_lines <- header_lines[stringr::str_detect(header_lines, "^%")]
  
  # Initialize metadata list
  metadata <- list(
    file_path = file_path,
    file_name = basename(file_path),
    tow_id = NA_character_,
    vessel = NA_character_,
    date_string = NA_character_,
    parsed_date = as.Date(NA),
    instrument_probes = list(),
    flow_meter_calibration = NA_real_,
    column_headers = character(),
    num_columns = NA_integer_
  )
  
  # Extract tow information and vessel
  tow_line <- comment_lines[stringr::str_detect(comment_lines, "^% Tow:")]
  if (length(tow_line) > 0) {
    tow_match <- stringr::str_match(tow_line, "^% Tow:\\s*([^\\s]+)\\s+(.+)$")
    if (!is.na(tow_match[1,2])) {
      metadata$tow_id <- stringr::str_trim(tow_match[1,2])
    }
    if (!is.na(tow_match[1,3])) {
      metadata$vessel <- stringr::str_trim(tow_match[1,3])
    }
  }
  
  # Extract date
  date_line <- comment_lines[stringr::str_detect(comment_lines, "^% Date:")]
  if (length(date_line) > 0) {
    date_match <- stringr::str_match(date_line, "^% Date:\\s*(.+)$")
    if (!is.na(date_match[1,2])) {
      metadata$date_string <- stringr::str_trim(date_match[1,2])
      
      # Parse date - BESS format: "13 Aug 2011" or "2 Sept 2012"
      # First, normalize "Sept" to "Sep" for proper parsing
      normalized_date <- stringr::str_replace(metadata$date_string, "Sept", "Sep")
      
      parsed_date <- tryCatch({
        lubridate::dmy(normalized_date)
      }, error = function(e) {
        # Try alternative parsing if first attempt fails
        tryCatch({
          lubridate::parse_date_time(normalized_date, orders = c("dmy", "dmY", "dby", "dbY"))
        }, error = function(e2) NA_real_)
      })
      
      if (!is.na(parsed_date)) {
        metadata$parsed_date <- as.Date(parsed_date)
      }
    }
  }
  
  # Extract instrument probe information
  probe_lines <- comment_lines[stringr::str_detect(comment_lines, "Probe #")]
  if (length(probe_lines) > 0) {
    probe_info <- purrr::map(probe_lines, function(line) {
      # Split on multiple instruments in one line
      # Pattern: "% Temperature Probe #  5164    Conductivity Probe #  3613"
      instruments <- stringr::str_split(line, "(?<=\\d)\\s+(?=[A-Z])")[[1]]
      
      purrr::map(instruments, function(inst_part) {
        probe_match <- stringr::str_match(inst_part, "([A-Z][a-z]+)\\s+Probe\\s*#\\s*(\\d+)")
        if (!is.na(probe_match[1,2])) {
          list(
            instrument = stringr::str_trim(probe_match[1,2]),
            probe_number = as.numeric(probe_match[1,3])
          )
        } else {
          NULL
        }
      })
    }) |>
    purrr::list_flatten() |>
    purrr::compact()
    
    metadata$instrument_probes <- probe_info
  }
  
  # Extract flow meter calibration
  flow_line <- comment_lines[stringr::str_detect(comment_lines, "Flow Meter Calibration")]
  if (length(flow_line) > 0) {
    flow_match <- stringr::str_match(flow_line, "([0-9.]+)\\s*\\(m/count\\)")
    if (!is.na(flow_match[1,2])) {
      metadata$flow_meter_calibration <- as.numeric(flow_match[1,2])
    }
  }
  
  # Extract column headers
  column_line <- comment_lines[stringr::str_detect(comment_lines, "^%\\s*(time\\s+pres|time\\s.*pres)")]
  if (length(column_line) > 0) {
    # Remove leading % and extra whitespace, then split on whitespace
    headers_clean <- stringr::str_remove(column_line[1], "^%\\s*")
    headers_clean <- stringr::str_trim(headers_clean)
    
    # Split on one or more whitespace characters
    column_names <- stringr::str_split(headers_clean, "\\s+")[[1]]
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
  comment_mask <- stringr::str_detect(all_lines, "^%")
  data_start <- which(!comment_mask)[1]
  
  if (is.na(data_start)) {
    rlang::abort(paste("No data found in file:", file_path))
  }
  
  # Read data portion with appropriate column names
  col_names <- metadata$column_headers
  if (length(col_names) == 0) {
    # Default column names if headers not found
    col_names <- c(
      "time",
      "pres",
      "echo",
      "temp",
      "theta",
      "sal",
      "sigma",
      "angle",
      "flow",
      "hzvel",
      "vtvel",
      "vol",
      "net",
      "fluor",
      "ptran",
      "oxygen",
      "IrC",
      "Irrad",
      "lat",
      "lon"
    )
  }
  
  # Read data using readr
  pro_data <- tryCatch(
    {
      data <- readr::read_table(
        file_path,
        skip = data_start - 1,
        col_names = col_names,
        col_types = readr::cols(.default = readr::col_double()),
        show_col_types = FALSE
      )
      
      # Rename columns to match SIO format if they exist
      if ("Irradcurrent" %in% names(data)) {
        data <- dplyr::rename(data, IrC = Irradcurrent)
      }
      if ("Irradiance" %in% names(data)) {
        data <- dplyr::rename(data, Irrad = Irradiance)
      }
      
      data
    },
    error = function(e) {
      rlang::abort(paste(
        "Failed to read data from file:",
        file_path,
        "Error:",
        e$message
      ))
    }
  )
  
  # Add metadata columns
  pro_data <- pro_data |>
    dplyr::mutate(
      file_name = metadata$file_name,
      tow_id = metadata$tow_id,
      vessel = metadata$vessel,
      date_string = metadata$date_string,
      parsed_date = metadata$parsed_date,
      # Add instrument probe information
      temperature_probe = {
        temp_probes <- sapply(metadata$instrument_probes, function(x) {
          if(!is.null(x) && x$instrument == "Temperature") as.character(x$probe_number) else NA_character_
        })
        temp_probes <- temp_probes[!is.na(temp_probes)]
        if(length(temp_probes) > 0) paste(temp_probes, collapse = ",") else NA_character_
      },
      conductivity_probe = {
        cond_probes <- sapply(metadata$instrument_probes, function(x) {
          if(!is.null(x) && x$instrument == "Conductivity") as.character(x$probe_number) else NA_character_
        })
        cond_probes <- cond_probes[!is.na(cond_probes)]
        if(length(cond_probes) > 0) paste(cond_probes, collapse = ",") else NA_character_
      },
      pressure_probe = {
        pres_probes <- sapply(metadata$instrument_probes, function(x) {
          if(!is.null(x) && x$instrument == "Pressure") as.character(x$probe_number) else NA_character_
        })
        pres_probes <- pres_probes[!is.na(pres_probes)]
        if(length(pres_probes) > 0) paste(pres_probes, collapse = ",") else NA_character_
      },
      oxygen_probe = {
        oxy_probes <- sapply(metadata$instrument_probes, function(x) {
          if(!is.null(x) && x$instrument == "Oxygen") as.character(x$probe_number) else NA_character_
        })
        oxy_probes <- oxy_probes[!is.na(oxy_probes)]
        if(length(oxy_probes) > 0) paste(oxy_probes, collapse = ",") else NA_character_
      },
      fluorometer = {
        fluor_probes <- sapply(metadata$instrument_probes, function(x) {
          if(!is.null(x) && x$instrument == "Fluorometer") as.character(x$probe_number) else NA_character_
        })
        fluor_probes <- fluor_probes[!is.na(fluor_probes)]
        if(length(fluor_probes) > 0) paste(fluor_probes, collapse = ",") else NA_character_
      },
      flow_meter_calibration = metadata$flow_meter_calibration,
      flow_meter_units = ifelse(!is.na(metadata$flow_meter_calibration), "(m/count)", NA_character_),
      .before = 1
    )
  
  # Convert decimal time to datetime if possible
  if ("time" %in% names(pro_data) && !is.na(metadata$parsed_date)) {
    # Extract year from parsed date for DOY conversion
    year <- lubridate::year(metadata$parsed_date)
    
    # Use convert_doy_to_datetime to properly handle decimal day of year
    pro_data <- pro_data |>
      dplyr::mutate(
        decimal_time = time,
        datetime_gmt = convert_doy_to_datetime(time, year, "GMT"),
        .after = time
      )
  }
  
  return(pro_data)
}


#' Load and process multiple BESS-formatted PRO files
#'
#' Batch processes BESS PRO files from specified directory or file list.
#' Includes comprehensive error handling.
#'
#' @param directory Character string. Directory path containing PRO files.
#'   Default is current directory (".")
#' @param pattern Character. File pattern to match (default: "\\.(PRO|pro)$")
#' @param daynight Logical. Whether to annotate the data with day-night 
#'   designation based on ship position and time of collection. Default is FALSE.
#' @param progress Logical. Show progress bar (default: TRUE)
#' @return Tibble with combined data from all processed files
#' @export
load_bess_pro_files <- function(
  directory = ".",
  pattern = "\\.(PRO|pro)$",
  daynight = FALSE,
  progress = TRUE
) {

  # Handle directory input (matching load_pro_files behavior)
  if (length(directory) == 1 && dir.exists(directory)) {
    file_paths <- list.files(directory, pattern = pattern, full.names = TRUE)
  } else {
    file_paths <- directory  # Assume it's a vector of file paths
  }

  # Validate files exist
  missing_files <- file_paths[!file.exists(file_paths)]
  if (length(missing_files) > 0) {
    rlang::abort(paste(
      "Files not found:",
      paste(missing_files, collapse = ", ")
    ))
  }

  if (length(file_paths) == 0) {
    rlang::abort("No files found to process")
  }

  # Set up processing function
  process_file <- function(file_path) {
    tryCatch(
      {
        ingest_bess_pro_file(file_path)
      },
      error = function(e) {
        rlang::warn(paste("Failed to process file", file_path, ":", e$message))
        NULL
      }
    )
  }

  # Process files sequentially (annotation parallelism handled by annotate_daytime)
  if (progress) {
    message("Processing ", length(file_paths), " files...")
  }
  results <- purrr::map(file_paths, process_file)

  # Remove failed files and combine results
  successful_results <- purrr::compact(results)

  if (length(successful_results) == 0) {
    rlang::abort("No files were successfully processed")
  }

  if (length(successful_results) < length(file_paths)) {
    n_failed <- length(file_paths) - length(successful_results)
    rlang::warn(paste(n_failed, "files failed to process"))
  }

  # Combine all data
  combined_data <- dplyr::bind_rows(successful_results)

  # Add timezone and local datetime if coordinates are available
  if (all(c("lat", "lon", "datetime_gmt") %in% names(combined_data))) {
    if (progress) {
      message("Adding timezone and local datetime...")
    }
    
    # Validate coordinates; handle invalid coordinates gracefully
    lat <- combined_data$lat
    lon <- combined_data$lon
    valid_mask <- !is.na(lat) & is.finite(lat) & lat >= -90 & lat <= 90 &
      !is.na(lon) & is.finite(lon) & lon >= -180 & lon <= 180

    if (!any(valid_mask)) {
      rlang::warn(
        "No valid coordinates found; defaulting timezone to UTC for datetime conversion"
      )
      tz_info <- list(
        timezone = rep("Etc/GMT", length.out = nrow(combined_data)),
        timezone_name = rep(NA_character_, length.out = nrow(combined_data)),
        utc_offset_hours = rep(0, length.out = nrow(combined_data))
      )
    } else {
      if (!all(valid_mask)) {
        rlang::warn(
          paste0(
            sum(!valid_mask),
            " rows have invalid coordinates; timezone inferred from valid coordinates only"
          )
        )
      }
      tz_info <- get_timezone_from_coords(lat, lon, warn_ocean = FALSE)
      if (progress) {
        message(
          "Detected local timezone offsets spanning: ",
          paste(range(na.omit(tz_info$utc_offset_hours)), collapse = " to "),
          " hours"
        )
      }
    }
    
    combined_data$timezone <- tz_info$timezone
    combined_data$datetime_local <- combined_data$datetime_gmt +
      lubridate::hours(tz_info$utc_offset_hours)
  }

  # Add day/night annotation if requested and possible
  if (
    daynight &&
      all(c("lat", "lon", "datetime_gmt") %in% names(combined_data))
  ) {
    if (progress) {
      message("Adding day/night annotations...")
    }

    # Use the existing annotate_daytime function
    combined_data <- annotate_daytime(combined_data)
  }

  if (progress) {
    message(
      "Successfully processed ",
      length(successful_results),
      " files with ",
      nrow(combined_data),
      " total observations"
    )
  }

  return(combined_data)
}