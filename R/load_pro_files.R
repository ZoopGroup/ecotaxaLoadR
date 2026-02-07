#' @title Load multiple PRO files
#'
#' @description Batch processes one to many MOCNESS PRO files in a directory
#' with error handling and comprehensive reporting.
#'
#' @note See \code{ingest_pro_file} in this package to harvest information from
#' a one, specific PRO file.
#'
#' @param directory Character string. Directory path containing PRO files.
#'   Default is current directory (".").
#' @param daynight Boolean. Whether to annotate the data with day-night 
#'   designation based on ship position and time of collection. Default is FALSE.
#'
#' @return A single data frame with all successfully processed PRO rows bound
#'   together. Attributes contain processing summary information:
#'   \item{failed_files}{Character vector of filenames that failed to process}
#'   \item{error_messages}{Character vector of corresponding error messages}
#'   \item{processing_summary}{List with detailed processing information}
#'
#' @details
#' This function:
#' \itemize{
#'   \item Searches for all .pro files in the specified directory
#'   \item Processes each file using \code{\link{ingest_pro_file}}
#'   \item Provides detailed progress reporting
#'   \item Handles errors gracefully and continues processing
#'   \item Returns comprehensive summary of successes and failures
#' }
#'
#' @examples
#' \dontrun{
#' # Process all PRO files in current directory
#' all_data <- load_pro_files()
#'
#' # Process files with day/night annotation
#' all_data <- load_pro_files(daynight = TRUE)
#'
#' # Process files in specific directory
#' all_data <- load_pro_files("path/to/pro/files")
#'
#' # Check for any failed files
#' attr(all_data, "failed_files")
#' }
#'
#' @importFrom purrr imap safely compact
#' @importFrom tools file_ext
#'
#' @seealso \code{\link{ingest_pro_file}} for processing individual files
#'
#' @export
load_pro_files <- function(
  directory = ".",
  daynight  = FALSE
  ) {

  # initialize tracking variables for failed files
  failed_files   <- character(0)
  error_messages <- character(0)

  all_files <- list.files(directory, full.names = TRUE)
  pro_files <- all_files[tolower(tools::file_ext(all_files)) == "pro"]

  if (length(pro_files) == 0) {
    stop("DNF PRO files in this directory: ", directory)
  }

  cat("Found", length(pro_files), "PRO files to process\n")
  if (daynight) {
    cat("Day/night annotation enabled\n")
  }
  cat("\n")

  # process each file using purrr::map with safe error handling
  pro_data_list <- purrr::imap(
    pro_files,
    ~ {
      file_name <- basename(.x)
      cat(
        "=== Processing file",
        .y,
        "of",
        length(pro_files),
        ":",
        file_name,
        "===\n"
      )

      # use safely to handle errors gracefully
      safe_ingest <- purrr::safely(ingest_pro_file)
      result      <- safe_ingest(.x, daynight = daynight)  # Pass daynight parameter

      if (!is.null(result$error)) {
        error_msg <- result$error$message
        cat("Error processing", file_name, ":", error_msg, "\n\n")

        # track failed files (using <<- to modify parent scope variables)
        failed_files <<- c(failed_files, file_name)
        error_messages <<- c(error_messages, error_msg)

        return(NULL)
      }

      return(result$result)
    }
  )

  # set names and remove NULL entries (failed files)
  names(pro_data_list) <- basename(pro_files)
  pro_data_list        <- purrr::compact(pro_data_list)

  if (length(pro_data_list) == 0) {
    stop("No PRO files were successfully processed in ", directory)
  }

  combined_data <- dplyr::bind_rows(pro_data_list)

  # report processing summary
  cat("=== PROCESSING SUMMARY ===\n")
  cat("Total files found:", length(pro_files), "\n")
  cat("Successfully processed:", length(pro_data_list), "\n")
  cat("Failed to process:", length(failed_files), "\n")

  if (length(failed_files) > 0) {
    cat("\nFailed files:\n")
    for (i in seq_along(failed_files)) {
      cat(sprintf(
        "  %d. %s\n     Error: %s\n",
        i,
        failed_files[i],
        error_messages[i]
      ))
    }
  } else {
    cat("All files processed successfully!\n")
  }
  cat("\n")

  # add failed files information as attributes to the result
  attr(combined_data, "failed_files") <- failed_files
  attr(combined_data, "error_messages") <- error_messages
  attr(combined_data, "processing_summary") <- list(
    total_files      = pro_files,
    successful_files = pro_data_list,
    failed_files     = failed_files
  )

  return(combined_data)
}


#' @title Load and process MOCNESS PRO file
#'
#' @description Function to ingest a MOCNESS PRO file, extracting metadata and
#' data, converting time formats, and adding timezone information based on
#' coordinates.
#'
#' @param file_path Character string. Path to the PRO file to process.
#' @param daynight (boolean) Boolean indicating whether to annotate the data
#'   with a day-night designation based on ship position and time of collection.
#'
#' @return A data frame containing the processed PRO file data with additional columns:
#'   \item{datetime_gmt}{POSIXct. Datetime in UTC}
#'   \item{datetime_local}{POSIXct. Local clock time (offset applied, tz stored separately)}
#'   \item{timezone}{Character. IANA timezone when available, else Etc/GMT±X}
#'   \item{file_name}{Character. Original filename}
#'   Plus all metadata fields and original data columns.
#'
#' @details
#' The function performs the following operations:
#' \itemize{
#'   \item Extracts metadata from the file header
#'   \item Reads tabular data starting from the "time" header line
#'   \item Converts decimal day-of-year time to proper datetime objects
#'   \item Determines local timezone from coordinates
#'   \item Adds comprehensive processing information
#' }
#'
#' @examples
#' \dontrun{
#' # Load a single PRO file
#' data <- ingest_pro_file("MOC1_01A.PRO")
#'
#' # View the structure
#' str(data)
#' }
#'
#' @importFrom readr read_lines read_table
#' @importFrom stringr str_detect
#' @importFrom purrr iwalk
#' @importFrom lubridate year with_tz
#'
#' @export
ingest_pro_file <- function(
  file_path,
  daynight = FALSE
  ) {
  cat("Processing file:", file_path, "\n")

  # extract metadata
  metadata <- extract_pro_metadata(file_path)
  cat(
    "Extracted metadata for Tow",
    metadata$tow_number,
    "on",
    as.character(metadata$date),
    "\n"
  )

  # Find the header line (starts with "time")
  all_lines       <- readr::read_lines(file_path)
  header_line_idx <- which(stringr::str_detect(all_lines, "^time\\s+"))

  if (length(header_line_idx) == 0) {
    stop("Could not find data header line starting with 'time'")
  }

  # read the data starting from the line after the header
  data <- readr::read_table(
    file = file_path,
    skip = header_line_idx - 1, # skip header lines
    col_names = TRUE,
    show_col_types = FALSE
  )

  cat("Read", nrow(data), "data rows with", ncol(data), "columns\n")

  # add header metadata as columns (repeat for each data row)
  if (length(metadata) > 0) {
    purrr::iwalk(
      metadata,
      ~ {
        data[[.y]] <<- .x
      }
    )
  }

  # add file metadata
  filename <- basename(file_path)
  data$file_name <- filename

  # extract MOC number from filename
  # Handle both "MOC" and "M" prefixes
  if (
    (grepl("(?i)moc", filename) || grepl("(?i)^m[0-9]+", filename)) &&
      grepl("[0-9]+", filename)
  ) {
    # Extract the first number found in the filename
    moc_number <- as.numeric(stringr::str_extract(filename, "[0-9]+"))
    data$moc <- moc_number
    cat("Extracted MOC number:", moc_number, "from filename\n")
  } else {
    data$moc <- NA_real_
    cat("MOC number not found in filename\n")
  }

  # convert time column to datetime
  if ("time" %in% names(data)) {
    data$datetime_gmt <- convert_doy_to_datetime(
      data$time,
      lubridate::year(metadata$date),
      "UTC"
    )

    tz_info <- get_timezone_from_coords(data$lat, data$lon)

    data$timezone <- tz_info$timezone
    data$datetime_local <- data$datetime_gmt + lubridate::hours(tz_info$utc_offset_hours)

    if (daynight == TRUE) {
      data <- annotate_daytime(data)
    }

  }

  cat("processed PRO file with", nrow(data), "records\n")
  cat(
    "Time range:",
    as.character(min(data$datetime_gmt, na.rm = TRUE)),
    "to",
    as.character(max(data$datetime_gmt, na.rm = TRUE)),
    "(GMT)\n"
  )
  cat(
    "Depth range:",
    round(min(data$pres, na.rm = TRUE), 2),
    "to",
    round(max(data$pres, na.rm = TRUE), 2),
    "meters\n"
  )
  cat(
    "Location range: Lat",
    round(min(data$lat, na.rm = TRUE), 4),
    "to",
    round(max(data$lat, na.rm = TRUE), 4),
    ", Lon",
    round(min(data$lon, na.rm = TRUE), 4),
    "to",
    round(max(data$lon, na.rm = TRUE), 4),
    "\n\n"
  )

  return(data)
}


#' @title Extract metadata from PRO file header
#'
#' @description Parses the header section of a MOCNESS PRO file to extract
#' metadata including tow information, date, instrument serial numbers, and
#' calibration data. Handles different tow line formats.
#'
#' @param file_path Character string. Path to the PRO file to process.
#'
#' @return A list containing extracted metadata with elements:
#'   \item{tow_number}{Numeric. The tow number}
#'   \item{vessel}{Character. Vessel name}
#'   \item{cruise_id}{Character. Cruise identifier}
#'   \item{date}{Date. Sampling date}
#'   \item{temperature_probe}{Character. Temperature probe serial number}
#'   \item{conductivity_probe}{Character. Conductivity probe serial number}
#'   \item{pressure_probe}{Character. Pressure probe serial number}
#'   \item{oxygen_probe}{Character. Oxygen probe serial number}
#'   \item{fluorometer}{Character. Fluorometer identifier}
#'   \item{flow_meter_calibration}{Numeric. Flow meter calibration value}
#'   \item{flow_meter_units}{Character. Flow meter units}
#'
#' @importFrom readr read_lines
#' @importFrom stringr str_split str_extract str_trim
#' @importFrom lubridate mdy
#'
#' @keywords internal
extract_pro_metadata <- function(file_path) {
  # read first few lines to get header information
  header_lines <- readr::read_lines(file_path, n_max = 10)

  # initialize metadata list
  metadata <- list()

  # extract tow information - handle different formats
  tow_line <- header_lines[grepl("^%.*Tow:", header_lines)]
  if (length(tow_line) > 0) {
    # Split by whitespace and remove empty elements
    tow_parts <- stringr::str_split(tow_line, "\\s+")[[1]]
    tow_parts <- tow_parts[tow_parts != ""]  # Remove empty strings
    
    if (length(tow_parts) >= 4) {
      metadata$tow_number <- as.numeric(tow_parts[3])
      
      # Handle different vessel/cruise formats
      if (length(tow_parts) == 5) {
        # Format: %	Tow:	1  AE  AE2214
        # tow_parts[4] = vessel (e.g., "AE")
        # tow_parts[5] = cruise (e.g., "AE2214")
        metadata$vessel <- stringr::str_trim(tow_parts[4])
        metadata$cruise_id <- stringr::str_trim(tow_parts[5])
      } else if (length(tow_parts) >= 6) {
        # Format: %	Tow:	13  Sally Ride  SR2408
        # tow_parts[4:5] = vessel (e.g., "Sally", "Ride")
        # tow_parts[6] = cruise (e.g., "SR2408")
        metadata$vessel <- paste(stringr::str_trim(tow_parts[4:5]), collapse = " ")
        metadata$cruise_id <- stringr::str_trim(tow_parts[6])
      } else {
        # Fallback: treat everything after tow number as vessel
        remaining_parts <- tow_parts[4:length(tow_parts)]
        if (length(remaining_parts) > 1) {
          # Assume last part is cruise_id, rest is vessel
          metadata$vessel <- paste(remaining_parts[1:(length(remaining_parts)-1)], collapse = " ")
          metadata$cruise_id <- remaining_parts[length(remaining_parts)]
        } else {
          # Only one remaining part - treat as vessel
          metadata$vessel <- remaining_parts[1]
          metadata$cruise_id <- NA_character_
        }
      }
    }
  }

  # extract date
  date_line <- header_lines[grepl("^%.*Date:", header_lines)]
  if (length(date_line) > 0) {
    date_str <- stringr::str_extract(date_line, "\\d+/\\d+/\\d+")
    metadata$date <- lubridate::mdy(date_str)
  }

  # extract instrument serial numbers
  temp_line <- header_lines[grepl("Temperature.*Probe", header_lines)]
  if (length(temp_line) > 0) {
    metadata$temperature_probe <- stringr::str_extract(temp_line, "\\d+")
  }

  cond_line <- header_lines[grepl("Conductivity.*Probe", header_lines)]
  if (length(cond_line) > 0) {
    metadata$conductivity_probe <- stringr::str_extract(cond_line, "\\d+$")
  }

  press_line <- header_lines[grepl("Pressure.*Probe", header_lines)]
  if (length(press_line) > 0) {
    metadata$pressure_probe <- stringr::str_extract(press_line, "\\d+")
  }

  oxygen_line <- header_lines[grepl("Oxygen.*Probe", header_lines)]
  if (length(oxygen_line) > 0) {
    metadata$oxygen_probe <- stringr::str_extract(oxygen_line, "\\d+$")
  }

  fluor_line <- header_lines[grepl("Fluorometer", header_lines)]
  if (length(fluor_line) > 0) {
    metadata$fluorometer <- stringr::str_extract(fluor_line, "[A-Z]+\\d+")
  }

  # extract flow meter calibration
  flow_line <- header_lines[grepl("Flow.*Meter.*Calibration", header_lines)]
  if (length(flow_line) > 0) {
    metadata$flow_meter_calibration <- as.numeric(stringr::str_extract(
      flow_line,
      "\\d+\\.\\d+"
    ))
    metadata$flow_meter_units <- stringr::str_extract(flow_line, "\\([^)]+\\)")
  }

  return(metadata)
}


#' @title Convert day of year decimal time to datetime
#'
#' @description Converts a decimal day of year format (e.g., 123.5 = day 123,
#' 12:00) to a proper datetime object.
#'
#' @param doy_decimal Numeric vector. Decimal day of year values.
#' @param year Numeric. The year for the conversion.
#' @param timezone Character. Timezone for the conversion. Default is "GMT".
#'
#' @return POSIXct datetime vector.
#'
#' @examples
#' \dontrun{
#' # Convert day 100.5 (April 10th, 12:00) of 2024 to datetime
#' convert_doy_to_datetime(100.5, 2024, "GMT")
#' }
#'
#' @importFrom lubridate as_datetime days seconds
#' @export
convert_doy_to_datetime <- function(
  doy_decimal,
  year,
  timezone = "GMT"
) {
  # split into day of year and decimal time
  doy <- floor(doy_decimal)
  decimal_time <- doy_decimal - doy

  # convert to datetime
  start_of_year <- lubridate::as_datetime(paste0(year, "-01-01"), tz = timezone)
  days_to_add <- doy - 1 # DOY 1 = Jan 1
  seconds_to_add <- decimal_time * 24 * 3600 # Convert decimal day to seconds

  datetime <- start_of_year +
    lubridate::days(days_to_add) +
    lubridate::seconds(seconds_to_add)

  return(datetime)
}


#' @title Get timezone from coordinates (vectorized)
#'
#' @description Determines timezones and UTC offsets per row using coordinates.
#' Results are cached on rounded coordinates to reduce repeated lookups.
#'
#' @param lat Numeric vector of latitudes.
#' @param lon Numeric vector of longitudes.
#' @param cache_precision Numeric. Degrees to round lat/lon before caching.
#'   Default 0.1 (about 11 km).
#' @param warn_ocean Logical. Emit a warning if any lookup returns a land
#'   timezone or NA. Default TRUE.
#'
#' @return A list with two equally long vectors:
#'   \item{timezone}{Character. IANA zone from lutz when available, otherwise fixed-offset Etc/GMT±X}
#'   \item{utc_offset_hours}{Numeric. Offset hours from UTC (west is negative)}
#'
#' @examples
#' # Get timezone info for coordinates in the Pacific
#' # tz_info <- ecotaxaLoadR::get_timezone_from_coords(34.0, -118.0)
#'
#' @importFrom lutz tz_lookup_coords
#'
#' @keywords internal
get_timezone_from_coords <- function(
  lat,
  lon,
  cache_precision = 0.1,
  warn_ocean = TRUE
) {
  n <- max(length(lat), length(lon))

  if (n == 0) {
    return(list(
      timezone = character(0),
      timezone_name = character(0),
      utc_offset_hours = numeric(0)
    ))
  }

  # recycle shorter input if needed
  lat <- rep_len(lat, n)
  lon <- rep_len(lon, n)

  valid <- is.finite(lat) & is.finite(lon) & lat >= -90 & lat <= 90 &
    lon >= -180 & lon <= 180

  offset_hours <- rep(NA_real_, n)
  offset_hours[valid] <- round(lon[valid] / 15)

  etc_label <- rep(NA_character_, n)
  etc_label[valid] <- paste0(
    "Etc/GMT",
    ifelse(offset_hours[valid] > 0, "-", "+"),
    abs(offset_hours[valid])
  )

  tz_lookup <- rep(NA_character_, n)
  timezone_name <- rep(NA_character_, n)

  if (any(valid)) {
    lat_r <- round(lat[valid] / cache_precision) * cache_precision
    lon_r <- round(lon[valid] / cache_precision) * cache_precision
    keys <- paste(lat_r, lon_r, sep = "|")
    unique_keys <- unique(keys)

    cache <- new.env(parent = emptyenv())
    for (k in unique_keys) {
      parts <- strsplit(k, "|", fixed = TRUE)[[1]]
      lat_k <- as.numeric(parts[1])
      lon_k <- as.numeric(parts[2])
      tz_val <- tryCatch(
        lutz::tz_lookup_coords(lat = lat_k, lon = lon_k, method = "accurate"),
        error = function(e) NA_character_
      )
      assign(k, tz_val, envir = cache)
    }

    tz_lookup[valid] <- vapply(
      keys,
      function(k) get(k, envir = cache, inherits = FALSE),
      character(1)
    )

    timezone_name <- ifelse(
      !is.na(tz_lookup) & !grepl("^Etc/", tz_lookup),
      tz_lookup,
      NA_character_
    )

    land_rows <- valid & !is.na(timezone_name)
    missing_rows <- valid & is.na(tz_lookup)

    if (warn_ocean && any(land_rows)) {
      warning(
        sprintf(
          "%s rows have non-ocean timezones; data are assumed to be oceanic",
          sum(land_rows)
        ),
        call. = FALSE
      )
    }
    if (warn_ocean && any(missing_rows)) {
      warning(
        sprintf(
          "%s rows had no timezone match; falling back to lon-derived offsets",
          sum(missing_rows)
        ),
        call. = FALSE
      )
    }
  }

  timezone_out <- ifelse(!is.na(tz_lookup), tz_lookup, etc_label)

  list(
    timezone = timezone_out,
    utc_offset_hours = offset_hours
  )
}